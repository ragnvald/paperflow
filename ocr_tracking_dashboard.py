#!/usr/bin/env python3
import base64
import datetime as dt
import hashlib
import json
import os
import queue
import re
import socket
import sqlite3
import ssl
import threading
import time
import tkinter as tk
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk

import ttkbootstrap as tb
from ttkbootstrap.constants import BOTH, END, LEFT, W, X

# Reuse API/token helpers from tracking script to stay aligned with one API path.
from init_ocr_tracking_db import (  # type: ignore
    DEFAULT_API_BASE_URL,
    DEFAULT_TOKEN_FILE,
    api_get_json,
    fetch_all_documents,
    normalize_document,
    normalize_base_url,
    normalize_token_header,
    read_token_file,
)

DATA_MEMORY_DIR = Path("data_memory")
DATA_OUT_DIR = Path("data_out")
API_OCR_HISTORY_PATH = DATA_MEMORY_DIR / "api_ocr_history.jsonl"
PIPELINE_DB_PATH = DATA_MEMORY_DIR / "ocr_pipeline.sqlite3"
RAG_INGESTION_ROOT = DATA_OUT_DIR / "rag_ingestion"
DASHBOARD_SETTINGS_PATH = DATA_MEMORY_DIR / "ocr_dashboard_settings.json"
DEFAULT_LLM_KEY_FILE = Path("secrets") / "openai.api"
LEGACY_LLM_KEY_FILE = Path("secrets") / "openai.token"
DEFAULT_LLM_TIMEOUT_SECONDS = 180
DEFAULT_LLM_RETRY_ATTEMPTS = 2
LLM_RETRY_BACKOFF_SECONDS = 2.0
SETTINGS_AUTOSAVE_DELAY_MS = 500

BATCH_OPTIONS = tuple([str(i) for i in range(5, 101, 5)] + ["250", "500", "1000"])
TASK_POLL_INTERVAL_SECONDS = 2.0
NO_TASK_DIFF_POLL_INTERVAL_SECONDS = 5.0
NO_TASK_DIFF_MAX_WAIT_SECONDS = 600.0
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
ENGINE_PAPERLESS = "paperless_internal"
ENGINE_LLM = "llm_openai_compatible"
LLM_MODE_RESPONSES = "responses"
LLM_MODE_CHAT = "chat_completions"
DEFAULT_LLM_PROMPT_TEXT = (
    "Extract all text from this PDF with high fidelity. "
    "Return plain markdown optimized for RAG chunking with headings where meaningful."
)
SUCCESS_SORT_FIELDS = (
    ("Run time", "run_ts"),
    ("ID", "id"),
    ("Title", "title"),
    ("Pre chars", "pre_content_length"),
    ("Post chars", "post_content_length"),
    ("Delta", "content_delta"),
    ("Status", "status"),
)
SUCCESS_SORT_ORDERS = ("Descending", "Ascending")


class OcrDashboard(tb.Window):
    def __init__(self) -> None:
        super().__init__(themename="flatly")
        self.title("Paperless OCR Control Center")
        self.geometry("1560x860")

        self.run_thread: threading.Thread | None = None
        self.api_run_active = False
        self.export_active = False
        self.stop_event = threading.Event()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.log_file_lock = threading.Lock()
        self.history_file_lock = threading.Lock()
        self.log_file_path = DATA_MEMORY_DIR / "dashboard.log"
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        self.history_file_path = API_OCR_HISTORY_PATH
        self.pipeline_db_path = PIPELINE_DB_PATH
        self.settings_file_path = DASHBOARD_SETTINGS_PATH
        self.rag_root_dir = RAG_INGESTION_ROOT
        self.pipeline_db_path.parent.mkdir(parents=True, exist_ok=True)
        self.rag_root_dir.mkdir(parents=True, exist_ok=True)
        self._settings_load_in_progress = False
        self._settings_autosave_after_id: str | None = None
        self.llm_api_mode_help_window: tk.Toplevel | None = None

        self.docs: list[dict] = []
        self.recent_manual_ids: set[int] = set()
        self.success_rows: list[dict] = []
        self.failed_rows: list[dict] = []

        self.selected_candidates: list[dict] = []
        self.prospective_rows: list[dict] = []
        self.pdf_search_rows: list[dict] = []
        self.pipeline_rows: list[dict] = []
        self.run_total = 0
        self.run_completed_ids: set[int] = set()
        self.run_started_ids: set[int] = set()
        self.progress_scope = "Idle"
        self.progress_text = tk.StringVar(value="Idle | 0/0 (0%) | Pending: 0")
        self.progress_value = tk.DoubleVar(value=0.0)
        self.paperless_fetch_status = tk.StringVar(value="Paperless overview last fetched: never")
        self.tree_sort_state: dict[str, dict[str, bool]] = {}
        self._ensure_pipeline_schema()

        self._build_ui()
        self._load_saved_settings()
        self._register_settings_autosave()
        self.refresh_pipeline_overview()
        self.protocol("WM_DELETE_WINDOW", self._on_window_close)
        self._append_file_log(f"\n===== DASHBOARD START {dt.datetime.now().isoformat()} =====\n")
        self.after(100, self._drain_log_queue)

    def _build_ui(self) -> None:
        root = tb.Frame(self, padding=10)
        root.pack(fill=BOTH, expand=True)

        self._build_step1_controls(root)

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=BOTH, expand=True, pady=(0, 6))

        self.tab_run = tb.Frame(self.notebook, padding=8)
        self.tab_pdf_search = tb.Frame(self.notebook, padding=8)
        self.tab_prospective = tb.Frame(self.notebook, padding=8)
        self.tab_pipeline = tb.Frame(self.notebook, padding=8)
        self.tab_rag = tb.Frame(self.notebook, padding=8)
        self.tab_success = tb.Frame(self.notebook, padding=8)
        self.tab_settings = tb.Frame(self.notebook, padding=8)
        self.tab_log = tb.Frame(self.notebook, padding=8)

        self.notebook.add(self.tab_run, text="Pipeline Run")
        self.notebook.add(self.tab_pdf_search, text="Search Documents")
        self.notebook.add(self.tab_prospective, text="Prospective Reruns")
        self.notebook.add(self.tab_pipeline, text="Pipeline Overview")
        self.notebook.add(self.tab_rag, text="RAG")
        self.notebook.add(self.tab_success, text="OCR History")
        self.notebook.add(self.tab_settings, text="Settings")
        self.notebook.add(self.tab_log, text="Activity Log")

        self._build_run_tab()
        self._build_pdf_search_tab()
        self._build_prospective_tab()
        self._build_pipeline_tab()
        self._build_rag_tab()
        self._build_success_tab()
        self._build_top_controls(self.tab_settings)
        self.log = ScrolledText(self.tab_log, wrap="word")
        self.log.pack(fill=BOTH, expand=True)

        status_row = tb.Frame(root)
        status_row.pack(fill=X, pady=(0, 2))
        tb.Label(status_row, text="Activity", bootstyle="secondary").pack(side=LEFT, padx=(0, 8))
        tb.Label(status_row, textvariable=self.progress_text, bootstyle="secondary").pack(side=LEFT, padx=(0, 10))
        tb.Progressbar(
            status_row,
            variable=self.progress_value,
            maximum=100.0,
            mode="determinate",
            bootstyle="success-striped",
        ).pack(side=LEFT, fill=X, expand=True)

    def _build_step1_controls(self, parent: tk.Widget) -> None:
        controls = tb.Labelframe(parent, text="Step 1: Candidate Set", padding=8)
        controls.pack(fill=X, pady=(0, 8))

        self.batch_size = tk.StringVar(value="50")
        self.recent_days = tk.StringVar(value="14")

        row = tb.Frame(controls)
        row.pack(fill=X)

        tb.Label(row, text="Batch size").pack(side=LEFT, padx=(0, 6))
        tb.Combobox(row, values=BATCH_OPTIONS, textvariable=self.batch_size, state="readonly", width=8).pack(
            side=LEFT, padx=(0, 12)
        )
        tb.Label(row, text="Exclude OCR in last days").pack(side=LEFT, padx=(0, 6))
        tb.Entry(row, textvariable=self.recent_days, width=8).pack(side=LEFT, padx=(0, 12))
        tb.Button(
            row,
            text="Fetch overview from Paperless",
            bootstyle="primary",
            command=self.refresh_all,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            row,
            text="Rebuild candidates (loaded data)",
            bootstyle="info",
            command=self.refresh_candidates,
        ).pack(side=LEFT, padx=(0, 8))

        tb.Label(controls, textvariable=self.paperless_fetch_status, bootstyle="secondary").pack(anchor=W, pady=(8, 2))
        tb.Label(
            controls,
            text=(
                "Fetch overview from Paperless calls the API and refreshes the local overview. "
                "Rebuild candidates only recalculates the run list from already loaded data."
            ),
            bootstyle="secondary",
        ).pack(anchor=W)

    def _build_top_controls(self, parent: tk.Widget) -> None:
        top = tb.Frame(parent)
        top.pack(fill=BOTH, expand=True)
        top.columnconfigure(0, weight=1)
        top.rowconfigure(0, weight=1)

        settings_grid = tb.Frame(top)
        settings_grid.grid(row=0, column=0, sticky="nsew")
        settings_grid.columnconfigure(0, weight=1, uniform="settings_col")
        settings_grid.columnconfigure(1, weight=1, uniform="settings_col")

        api_frame = tb.Labelframe(settings_grid, text="Paperless API", padding=8)
        api_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))

        self.api_base_url = tk.StringVar(value=DEFAULT_API_BASE_URL)
        self.api_token = tk.StringVar(value="")
        self.verify_tls = tk.BooleanVar(value=False)
        self.page_size = tk.StringVar(value="200")
        self.timeout = tk.StringVar(value="30")

        self._add_form_row(api_frame, 0, "Base URL", self.api_base_url)
        self._add_form_row(api_frame, 1, "Token (optional)", self.api_token, show="*")
        self._add_form_row(api_frame, 2, "Page Size", self.page_size)
        self._add_form_row(api_frame, 3, "Timeout (s)", self.timeout)
        tb.Checkbutton(
            api_frame,
            text="Verify TLS",
            variable=self.verify_tls,
            bootstyle="round-toggle",
        ).grid(row=4, column=1, sticky=W, padx=6, pady=4)
        api_frame.columnconfigure(1, weight=1)

        llm_frame = tb.Labelframe(settings_grid, text="LLM OCR (OpenAI-Compatible)", padding=8)
        llm_frame.grid(row=0, column=1, sticky="nsew", padx=(6, 0))

        self.llm_api_base_url = tk.StringVar(value="https://api.openai.com")
        self.llm_api_key = tk.StringVar(value="")
        self.llm_model = tk.StringVar(value="gpt-4.1-mini")
        self.llm_timeout = tk.StringVar(value=str(DEFAULT_LLM_TIMEOUT_SECONDS))
        self.llm_retry_attempts = tk.StringVar(value=str(DEFAULT_LLM_RETRY_ATTEMPTS))
        self.llm_mode = tk.StringVar(value=LLM_MODE_RESPONSES)
        self.llm_prompt = tk.StringVar(value=DEFAULT_LLM_PROMPT_TEXT)
        self.llm_update_paperless = tk.BooleanVar(value=False)

        self._add_form_row(llm_frame, 0, "LLM Base URL", self.llm_api_base_url)
        self._add_form_row(llm_frame, 1, "LLM API Key (optional)", self.llm_api_key, show="*")
        self._add_form_row(llm_frame, 2, "Model", self.llm_model)
        self._add_form_row(llm_frame, 3, "LLM Timeout (s)", self.llm_timeout)
        self._add_form_row(llm_frame, 4, "Retry attempts", self.llm_retry_attempts)
        tb.Label(llm_frame, text="Prompt").grid(row=5, column=0, sticky=W, padx=6, pady=4)
        self.llm_prompt_text = tk.Text(llm_frame, height=4, wrap="word")
        self.llm_prompt_text.grid(row=5, column=1, sticky="ew", padx=6, pady=4)
        self.llm_prompt_text.insert("1.0", self.llm_prompt.get())
        self.llm_prompt_text.edit_modified(False)
        self.llm_prompt_text.bind("<<Modified>>", self._on_llm_prompt_text_modified)

        tb.Label(llm_frame, text="API mode").grid(row=6, column=0, sticky=W, padx=6, pady=4)
        tb.Combobox(
            llm_frame,
            values=(LLM_MODE_RESPONSES, LLM_MODE_CHAT),
            textvariable=self.llm_mode,
            state="readonly",
            width=22,
        ).grid(row=6, column=1, sticky=W, padx=6, pady=4)
        api_mode_info = tb.Label(
            llm_frame,
            text="ⓘ",
            bootstyle="info",
            cursor="hand2",
        )
        api_mode_info.grid(row=6, column=2, sticky=W, padx=(0, 6), pady=4)
        api_mode_info.bind("<Button-1>", lambda _event: self.show_llm_api_mode_info())
        tb.Checkbutton(
            llm_frame,
            text="Update Paperless content after successful LLM OCR",
            variable=self.llm_update_paperless,
            bootstyle="round-toggle",
        ).grid(row=7, column=1, sticky=W, padx=6, pady=4)
        llm_frame.columnconfigure(1, weight=1)

        footer = tb.Frame(top)
        footer.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        footer.columnconfigure(0, weight=1)
        tb.Label(
            footer,
            text=(
                f"Settings are auto-saved to {self.settings_file_path}. "
                f"API keys are read/written via {DEFAULT_TOKEN_FILE} and {DEFAULT_LLM_KEY_FILE}."
            ),
            bootstyle="secondary",
        ).grid(row=0, column=0, sticky=W, pady=(0, 8))
        actions = tb.Frame(footer)
        actions.grid(row=1, column=0, sticky=W)
        tb.Button(
            actions,
            text="Save",
            bootstyle="success",
            command=self.save_settings_now,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            actions,
            text="Clear settings",
            bootstyle="danger-outline",
            command=self.clear_settings,
        ).pack(side=LEFT)

    def show_llm_api_mode_info(self) -> None:
        if self.llm_api_mode_help_window is not None and self.llm_api_mode_help_window.winfo_exists():
            self.llm_api_mode_help_window.lift()
            self.llm_api_mode_help_window.focus_force()
            return

        win = tk.Toplevel(self)
        self.llm_api_mode_help_window = win
        win.title("API mode help")
        win.geometry("700x420")
        win.minsize(560, 320)
        win.transient(self)
        win.grab_set()

        outer = tb.Frame(win, padding=12)
        outer.pack(fill=BOTH, expand=True)

        tb.Label(
            outer,
            text="LLM API mode: what the two options mean",
            bootstyle="primary",
        ).pack(anchor=W, pady=(0, 8))

        body = ScrolledText(outer, wrap="word", height=14)
        body.pack(fill=BOTH, expand=True)
        body.insert(
            "1.0",
            (
                "responses\n"
                "- Endpoint: /v1/responses\n"
                "- Recommended default for modern OpenAI-compatible APIs.\n"
                "- This mode sends structured input, including prompt text and PDF file data URL.\n"
                "- Better fit for multimodal and newer response formats.\n\n"
                "chat_completions\n"
                "- Endpoint: /v1/chat/completions\n"
                "- Compatibility mode for providers that still expose classic chat format.\n"
                "- Prompt and PDF data are sent as chat message content.\n"
                "- Useful fallback when /v1/responses is unsupported.\n\n"
                "How to choose\n"
                "- Start with responses.\n"
                "- If your provider/model does not support it, switch to chat_completions."
            ),
        )
        body.configure(state="disabled")

        tb.Button(outer, text="Close", bootstyle="secondary", command=self._close_llm_api_mode_help).pack(
            anchor="e",
            pady=(8, 0),
        )
        win.protocol("WM_DELETE_WINDOW", self._close_llm_api_mode_help)

    def _close_llm_api_mode_help(self) -> None:
        if self.llm_api_mode_help_window is None:
            return
        if self.llm_api_mode_help_window.winfo_exists():
            self.llm_api_mode_help_window.destroy()
        self.llm_api_mode_help_window = None

    def _build_rag_tab(self) -> None:
        top = tb.Frame(self.tab_rag)
        top.pack(fill=X)

        tb.Label(
            top,
            text="RAG workflows and controls live here.",
            bootstyle="secondary",
        ).pack(anchor=W, pady=(0, 8))

        export_frame = tb.Labelframe(top, text="RAG Export", padding=8)
        export_frame.pack(fill=X)

        self.export_root_dir = tk.StringVar(value=str(self.rag_root_dir))
        self.export_source_mode = tk.StringVar(value=ENGINE_PAPERLESS)
        self.export_format_mode = tk.StringVar(value="both")

        self._add_form_row(export_frame, 0, "Export root", self.export_root_dir)
        tb.Label(export_frame, text="Source").grid(row=1, column=0, sticky=W, padx=6, pady=4)
        tb.Combobox(
            export_frame,
            values=(ENGINE_PAPERLESS, ENGINE_LLM),
            textvariable=self.export_source_mode,
            state="readonly",
            width=26,
        ).grid(row=1, column=1, sticky=W, padx=6, pady=4)
        tb.Label(export_frame, text="Format").grid(row=2, column=0, sticky=W, padx=6, pady=4)
        tb.Combobox(
            export_frame,
            values=("both", "md_only", "json_only"),
            textvariable=self.export_format_mode,
            state="readonly",
            width=16,
        ).grid(row=2, column=1, sticky=W, padx=6, pady=4)
        actions = tb.Frame(export_frame)
        actions.grid(row=3, column=1, sticky=W, padx=6, pady=(6, 2))
        tb.Button(
            actions,
            text="Export Selected Now",
            bootstyle="success",
            command=self.export_from_active_tab,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            actions,
            text="Go To Pipeline Run",
            bootstyle="secondary",
            command=lambda: self.notebook.select(self.tab_run),
        ).pack(side=LEFT)
        export_frame.columnconfigure(1, weight=1)

    def _add_form_row(
        self,
        parent: tk.Widget,
        row: int,
        label: str,
        variable: tk.StringVar,
        show: str | None = None,
    ) -> None:
        tb.Label(parent, text=label).grid(row=row, column=0, sticky=W, padx=6, pady=4)
        tb.Entry(parent, textvariable=variable, show=show).grid(row=row, column=1, sticky="ew", padx=6, pady=4)

    def _setting_string_vars(self) -> dict[str, tk.StringVar]:
        return {
            "api_base_url": self.api_base_url,
            "page_size": self.page_size,
            "timeout": self.timeout,
            "llm_api_base_url": self.llm_api_base_url,
            "llm_model": self.llm_model,
            "llm_timeout": self.llm_timeout,
            "llm_retry_attempts": self.llm_retry_attempts,
            "llm_mode": self.llm_mode,
            "llm_prompt": self.llm_prompt,
            "export_root_dir": self.export_root_dir,
            "export_source_mode": self.export_source_mode,
            "export_format_mode": self.export_format_mode,
            "batch_size": self.batch_size,
            "recent_days": self.recent_days,
            "prospective_threshold": self.prospective_threshold,
            "prospective_recent_days": self.prospective_recent_days,
            "pdf_query": self.pdf_query,
            "pdf_modified_contains": self.pdf_modified_contains,
            "pdf_exclude_recent_days": self.pdf_exclude_recent_days,
            "pdf_min_chars": self.pdf_min_chars,
            "pdf_max_chars": self.pdf_max_chars,
            "pdf_min_pages": self.pdf_min_pages,
            "pdf_max_pages": self.pdf_max_pages,
            "ocr_engine_mode": self.ocr_engine_mode,
            "paperless_fetch_status": self.paperless_fetch_status,
            "success_sort_field": self.success_sort_field,
            "success_sort_order": self.success_sort_order,
        }

    def _setting_bool_vars(self) -> dict[str, tk.BooleanVar]:
        return {
            "verify_tls": self.verify_tls,
            "llm_update_paperless": self.llm_update_paperless,
            "pdf_missing_archive_only": self.pdf_missing_archive_only,
        }

    def _settings_autosave_vars(self) -> list[tk.Variable]:
        vars_to_watch = list(self._setting_string_vars().values())
        vars_to_watch.extend(self._setting_bool_vars().values())
        vars_to_watch.extend([self.api_token, self.llm_api_key])
        return vars_to_watch

    def _to_bool(self, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return False

    def _default_string_settings(self) -> dict[str, str]:
        return {
            "api_base_url": DEFAULT_API_BASE_URL,
            "page_size": "200",
            "timeout": "30",
            "llm_api_base_url": "https://api.openai.com",
            "llm_model": "gpt-4.1-mini",
            "llm_timeout": str(DEFAULT_LLM_TIMEOUT_SECONDS),
            "llm_retry_attempts": str(DEFAULT_LLM_RETRY_ATTEMPTS),
            "llm_mode": LLM_MODE_RESPONSES,
            "llm_prompt": DEFAULT_LLM_PROMPT_TEXT,
            "export_root_dir": str(self.rag_root_dir),
            "export_source_mode": ENGINE_PAPERLESS,
            "export_format_mode": "both",
            "batch_size": "50",
            "recent_days": "14",
            "prospective_threshold": "120",
            "prospective_recent_days": "14",
            "pdf_query": "",
            "pdf_modified_contains": "",
            "pdf_exclude_recent_days": "0",
            "pdf_min_chars": "",
            "pdf_max_chars": "",
            "pdf_min_pages": "",
            "pdf_max_pages": "",
            "ocr_engine_mode": ENGINE_PAPERLESS,
            "paperless_fetch_status": "Paperless overview last fetched: never",
            "success_sort_field": SUCCESS_SORT_FIELDS[0][0],
            "success_sort_order": SUCCESS_SORT_ORDERS[0],
        }

    def _default_bool_settings(self) -> dict[str, bool]:
        return {
            "verify_tls": False,
            "llm_update_paperless": False,
            "pdf_missing_archive_only": False,
        }

    def _on_llm_prompt_text_modified(self, _event: object = None) -> None:
        if not hasattr(self, "llm_prompt_text"):
            return
        if not self.llm_prompt_text.edit_modified():
            return
        self.llm_prompt_text.edit_modified(False)
        prompt = self.llm_prompt_text.get("1.0", "end-1c")
        if self.llm_prompt.get() != prompt:
            self.llm_prompt.set(prompt)

    def _set_llm_prompt_text_widget(self, prompt: str) -> None:
        if not hasattr(self, "llm_prompt_text"):
            return
        current = self.llm_prompt_text.get("1.0", "end-1c")
        if current == prompt:
            return
        self.llm_prompt_text.delete("1.0", END)
        self.llm_prompt_text.insert("1.0", prompt)
        self.llm_prompt_text.edit_modified(False)

    def _delete_file_if_exists(self, path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError as exc:
            self._append_file_log(f"[WARN] Failed to delete file {path}: {exc}\n")

    def save_settings_now(self) -> None:
        if self._save_settings(show_error=True):
            messagebox.showinfo("Settings saved", f"Saved settings to {self.settings_file_path}.")
            self._emit(f"[INFO] Settings saved to {self.settings_file_path}\n")

    def clear_settings(self) -> None:
        confirmed = messagebox.askyesno(
            "Clear settings",
            (
                "This resets settings to defaults and clears saved API keys.\n\n"
                "Continue?"
            ),
        )
        if not confirmed:
            return

        self._settings_load_in_progress = True
        try:
            string_defaults = self._default_string_settings()
            for key, var in self._setting_string_vars().items():
                var.set(string_defaults.get(key, ""))

            bool_defaults = self._default_bool_settings()
            for key, var in self._setting_bool_vars().items():
                var.set(bool_defaults.get(key, False))

            self.api_token.set("")
            self.llm_api_key.set("")
            self._set_llm_prompt_text_widget(self.llm_prompt.get())
        finally:
            self._settings_load_in_progress = False

        self._delete_file_if_exists(self.settings_file_path)
        self._delete_file_if_exists(DEFAULT_TOKEN_FILE)
        self._delete_file_if_exists(DEFAULT_LLM_KEY_FILE)
        self._delete_file_if_exists(LEGACY_LLM_KEY_FILE)
        self._save_settings(show_error=False)
        self._emit("[INFO] Cleared settings and reset to defaults.\n")
        messagebox.showinfo("Settings cleared", "Settings reset to defaults and saved keys were removed.")

    def _load_saved_settings(self) -> None:
        self._settings_load_in_progress = True
        try:
            if self.settings_file_path.exists():
                raw = self.settings_file_path.read_text(encoding="utf-8").strip()
                if raw:
                    payload = json.loads(raw)
                    if not isinstance(payload, dict):
                        raise RuntimeError("Settings file content is not a JSON object")
                    for key, var in self._setting_string_vars().items():
                        if key not in payload:
                            continue
                        value = payload.get(key)
                        if value is None:
                            continue
                        var.set(str(value))
                    for key, var in self._setting_bool_vars().items():
                        if key not in payload:
                            continue
                        var.set(self._to_bool(payload.get(key)))

            if not self.api_token.get().strip():
                token = read_token_file(DEFAULT_TOKEN_FILE)
                if token:
                    self.api_token.set(token)

            if not self.llm_api_key.get().strip():
                llm_key = read_token_file(DEFAULT_LLM_KEY_FILE)
                if not llm_key:
                    llm_key = read_token_file(LEGACY_LLM_KEY_FILE)
                if llm_key:
                    self.llm_api_key.set(llm_key)
            self._set_llm_prompt_text_widget(self.llm_prompt.get())
        except Exception as exc:
            self._append_file_log(f"[WARN] Failed to load dashboard settings: {exc}\n")
        finally:
            self._settings_load_in_progress = False

    def _register_settings_autosave(self) -> None:
        for var in self._settings_autosave_vars():
            var.trace_add("write", self._schedule_settings_autosave)

    def _schedule_settings_autosave(self, *_: object) -> None:
        if self._settings_load_in_progress:
            return
        if self._settings_autosave_after_id is not None:
            try:
                self.after_cancel(self._settings_autosave_after_id)
            except Exception:
                pass
        self._settings_autosave_after_id = self.after(
            SETTINGS_AUTOSAVE_DELAY_MS,
            self._save_settings_autosave_callback,
        )

    def _save_settings_autosave_callback(self) -> None:
        self._settings_autosave_after_id = None
        self._save_settings(show_error=False)

    def _write_secret_file(self, path: Path, secret: str) -> None:
        value = (secret or "").strip()
        if not value:
            return
        try:
            current = read_token_file(path)
        except OSError:
            current = ""
        if current == value:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value + "\n", encoding="utf-8")
            os.chmod(path, 0o600)
        except OSError as exc:
            self._append_file_log(f"[WARN] Failed to write secret file {path}: {exc}\n")

    def _save_settings(self, show_error: bool = False) -> bool:
        try:
            payload: dict[str, str | bool] = {}
            for key, var in self._setting_string_vars().items():
                payload[key] = var.get()
            for key, var in self._setting_bool_vars().items():
                payload[key] = bool(var.get())

            self.settings_file_path.parent.mkdir(parents=True, exist_ok=True)
            self.settings_file_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._write_secret_file(DEFAULT_TOKEN_FILE, self.api_token.get())
            self._write_secret_file(DEFAULT_LLM_KEY_FILE, self.llm_api_key.get())
            return True
        except Exception as exc:
            if show_error:
                messagebox.showerror("Save settings failed", str(exc))
            self._append_file_log(f"[WARN] Failed to save dashboard settings: {exc}\n")
            return False

    def _build_run_tab(self) -> None:
        self.ocr_engine_mode = tk.StringVar(value=ENGINE_PAPERLESS)

        run_actions = tb.Labelframe(self.tab_run, text="Step 2: OCR + Export", padding=8)
        run_actions.pack(fill=X, pady=(0, 6))

        tb.Label(run_actions, text="OCR engine").pack(side=LEFT, padx=(0, 6))
        tb.Combobox(
            run_actions,
            values=(ENGINE_PAPERLESS, ENGINE_LLM),
            textvariable=self.ocr_engine_mode,
            state="readonly",
            width=24,
        ).pack(side=LEFT, padx=(0, 12))

        tb.Button(
            run_actions,
            text="Run OCR on Selection",
            bootstyle="success",
            command=self.run_selected_ocr,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            run_actions,
            text="Export Selection to RAG",
            bootstyle="info",
            command=self.export_selected_to_rag,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            run_actions,
            text="Stop",
            bootstyle="danger",
            command=self.stop_run,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            run_actions,
            text="Clear Log",
            bootstyle="secondary",
            command=self.clear_log,
        ).pack(side=LEFT, padx=(0, 8))

        selection_actions = tb.Labelframe(self.tab_run, text="Selection", padding=6)
        selection_actions.pack(fill=X, pady=(0, 6))
        tb.Button(
            selection_actions,
            text="Select All",
            bootstyle="secondary",
            command=self.select_all_run_rows,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            selection_actions,
            text="Clear Selection",
            bootstyle="secondary",
            command=self.clear_run_selection,
        ).pack(side=LEFT, padx=(0, 8))

        self.run_summary = tk.StringVar(value="No candidate set built yet")
        tb.Label(self.tab_run, textvariable=self.run_summary, bootstyle="secondary").pack(anchor=W, pady=(8, 6))
        tb.Label(
            self.tab_run,
            text=(
                "Pipeline modes: "
                "'paperless_internal' reprocesses via Paperless; "
                "'llm_openai_compatible' performs external OCR and can optionally patch Paperless content."
            ),
            bootstyle="info",
        ).pack(anchor=W, pady=(0, 6))

        tb.Label(self.tab_run, textvariable=self.progress_text, bootstyle="info").pack(anchor=W, pady=(0, 4))
        tb.Progressbar(
            self.tab_run,
            variable=self.progress_value,
            maximum=100.0,
            mode="determinate",
            bootstyle="success-striped",
        ).pack(fill=X, pady=(0, 8))

        self.run_tree = self._build_tree(
            self.tab_run,
            columns=("id", "title", "content_length", "modified", "last_manual_ocr"),
            headings=("ID", "Title", "Chars", "Modified", "Last manual OCR"),
        )

    def _build_pipeline_tab(self) -> None:
        controls = tb.Frame(self.tab_pipeline)
        controls.pack(fill=X, pady=(0, 6))
        tb.Button(
            controls,
            text="Refresh Overview",
            bootstyle="info",
            command=self.refresh_pipeline_overview,
        ).pack(side=LEFT, padx=(0, 8))
        tb.Button(
            controls,
            text="Export Selected to RAG",
            bootstyle="primary",
            command=self.export_pipeline_selected_to_rag,
        ).pack(side=LEFT, padx=(0, 8))

        self.pipeline_summary = tk.StringVar(value="No pipeline events yet")
        tb.Label(self.tab_pipeline, textvariable=self.pipeline_summary, bootstyle="secondary").pack(anchor=W, pady=(0, 6))

        self.pipeline_tree = self._build_tree(
            self.tab_pipeline,
            columns=("event_ts", "doc_id", "title", "action", "engine", "status", "paperless_update", "rag_md", "rag_json"),
            headings=("Timestamp", "Doc ID", "Title", "Action", "Engine", "Status", "Paperless update", "MD", "JSON"),
        )

    def _build_success_tab(self) -> None:
        controls = tb.Frame(self.tab_success)
        controls.pack(fill=X, pady=(0, 6))
        tb.Button(
            controls,
            text="Refresh Successful OCR",
            bootstyle="info",
            command=self.refresh_success_history_only,
        ).pack(side=LEFT)

        self.success_sort_field = tk.StringVar(value=SUCCESS_SORT_FIELDS[0][0])
        self.success_sort_order = tk.StringVar(value=SUCCESS_SORT_ORDERS[0])

        tb.Label(controls, text="Sort by").pack(side=LEFT, padx=(12, 6))
        success_sort_field_box = tb.Combobox(
            controls,
            values=tuple(label for label, _ in SUCCESS_SORT_FIELDS),
            textvariable=self.success_sort_field,
            state="readonly",
            width=14,
        )
        success_sort_field_box.pack(side=LEFT, padx=(0, 12))
        success_sort_field_box.bind("<<ComboboxSelected>>", lambda _event: self.refresh_success_tab())

        tb.Label(controls, text="Order").pack(side=LEFT, padx=(0, 6))
        success_sort_order_box = tb.Combobox(
            controls,
            values=SUCCESS_SORT_ORDERS,
            textvariable=self.success_sort_order,
            state="readonly",
            width=12,
        )
        success_sort_order_box.pack(side=LEFT)
        success_sort_order_box.bind("<<ComboboxSelected>>", lambda _event: self.refresh_success_tab())

        self.success_summary = tk.StringVar(value="No data loaded")
        tb.Label(self.tab_success, textvariable=self.success_summary, bootstyle="secondary").pack(anchor=W, pady=(0, 6))

        self.success_tree = self._build_tree(
            self.tab_success,
            columns=("run_ts", "id", "title", "pre", "post", "delta", "status"),
            headings=("Run", "ID", "Title", "Pre chars", "Post chars", "Delta", "Status"),
        )

    def _build_prospective_tab(self) -> None:
        controls = tb.Frame(self.tab_prospective)
        controls.pack(fill=X)

        self.prospective_threshold = tk.StringVar(value="120")
        self.prospective_recent_days = tk.StringVar(value="14")

        tb.Label(controls, text="Chars <").pack(side=LEFT, padx=(0, 6))
        tb.Entry(controls, textvariable=self.prospective_threshold, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Label(controls, text="Exclude manual OCR last days").pack(side=LEFT, padx=(0, 6))
        tb.Entry(controls, textvariable=self.prospective_recent_days, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Button(controls, text="Refresh Prospective", command=self.refresh_prospective, bootstyle="info").pack(side=LEFT)
        self.transfer_to_run_button = tb.Button(
            controls,
            text="Transfer to Run OCR",
            command=self.transfer_prospective_to_run,
            bootstyle="primary",
        )
        self.transfer_to_run_button.pack(side=LEFT, padx=(10, 0))

        self.prospective_summary = tk.StringVar(value="No data loaded")
        tb.Label(self.tab_prospective, textvariable=self.prospective_summary, bootstyle="secondary").pack(
            anchor=W, pady=(8, 6)
        )

        self.prospective_tree = self._build_tree(
            self.tab_prospective,
            columns=("id", "title", "content_length", "reason", "last_manual_ocr"),
            headings=("ID", "Title", "Chars", "Reason", "Last manual OCR"),
        )
        self._update_control_states()

    def _build_pdf_search_tab(self) -> None:
        filters_row_1 = tb.Frame(self.tab_pdf_search)
        filters_row_1.pack(fill=X)

        self.pdf_query = tk.StringVar(value="")
        self.pdf_modified_contains = tk.StringVar(value="")
        self.pdf_missing_archive_only = tk.BooleanVar(value=False)
        self.pdf_exclude_recent_days = tk.StringVar(value="0")

        tb.Label(filters_row_1, text="Search").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_1, textvariable=self.pdf_query, width=28).pack(side=LEFT, padx=(0, 12))

        tb.Label(filters_row_1, text="Modified contains").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_1, textvariable=self.pdf_modified_contains, width=18).pack(side=LEFT, padx=(0, 12))

        tb.Checkbutton(
            filters_row_1,
            text="Missing archive only",
            variable=self.pdf_missing_archive_only,
            bootstyle="round-toggle",
        ).pack(side=LEFT, padx=(0, 12))

        tb.Label(filters_row_1, text="Exclude OCR last days").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_1, textvariable=self.pdf_exclude_recent_days, width=6).pack(side=LEFT, padx=(0, 12))

        filters_row_2 = tb.Frame(self.tab_pdf_search)
        filters_row_2.pack(fill=X, pady=(8, 0))

        self.pdf_min_chars = tk.StringVar(value="")
        self.pdf_max_chars = tk.StringVar(value="")
        self.pdf_min_pages = tk.StringVar(value="")
        self.pdf_max_pages = tk.StringVar(value="")

        tb.Label(filters_row_2, text="Chars min").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_2, textvariable=self.pdf_min_chars, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Label(filters_row_2, text="Chars max").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_2, textvariable=self.pdf_max_chars, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Label(filters_row_2, text="Pages min").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_2, textvariable=self.pdf_min_pages, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Label(filters_row_2, text="Pages max").pack(side=LEFT, padx=(0, 6))
        tb.Entry(filters_row_2, textvariable=self.pdf_max_pages, width=8).pack(side=LEFT, padx=(0, 12))

        tb.Button(filters_row_2, text="Search Documents", command=self.refresh_pdf_search, bootstyle="info").pack(
            side=LEFT, padx=(0, 8)
        )
        tb.Button(filters_row_2, text="Reset Filters", command=self.reset_pdf_search_filters, bootstyle="secondary").pack(
            side=LEFT, padx=(0, 8)
        )
        tb.Button(
            filters_row_2,
            text="Export Selected to RAG",
            bootstyle="success",
            command=self.export_pdf_search_selected_to_rag,
        ).pack(side=LEFT, padx=(0, 8))
        self.transfer_pdf_to_run_button = tb.Button(
            filters_row_2,
            text="Transfer to Run OCR",
            command=self.transfer_pdf_search_to_run,
            bootstyle="primary",
        )
        self.transfer_pdf_to_run_button.pack(side=LEFT)

        self.pdf_summary = tk.StringVar(value="No data loaded")
        tb.Label(self.tab_pdf_search, textvariable=self.pdf_summary, bootstyle="secondary").pack(anchor=W, pady=(8, 6))

        self.pdf_tree = self._build_tree(
            self.tab_pdf_search,
            columns=("id", "title", "content_length", "page_count", "modified", "archive_filename", "original_filename"),
            headings=("ID", "Title", "Chars", "Pages", "Modified", "Archive file", "Original file"),
        )
        self._update_control_states()

    def _build_tree(self, parent: tk.Widget, columns: tuple[str, ...], headings: tuple[str, ...]) -> ttk.Treeview:
        frame = tb.Frame(parent)
        frame.pack(fill=BOTH, expand=True)

        tree = ttk.Treeview(frame, columns=columns, show="headings", height=18, selectmode="extended")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree_sort_state[str(tree)] = {col: True for col in columns}
        for col, head in zip(columns, headings):
            tree.heading(
                col,
                text=head,
                anchor="w",
                command=lambda c=col, t=tree: self._sort_tree_by_column(t, c),
            )
            width = 120
            if col in ("title", "reason"):
                width = 420
            elif col in ("archive_filename", "original_filename"):
                width = 320
            elif col in ("rag_md", "rag_json"):
                width = 280
            elif col in ("modified", "last_manual_ocr", "run_ts", "event_ts"):
                width = 180
            tree.column(col, width=width, anchor="w")

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        return tree

    def _sort_key(self, raw: str):
        text = (raw or "").strip()
        if not text:
            return (3, "")
        # numeric
        try:
            return (0, int(text))
        except ValueError:
            pass
        # datetime-like
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d_%H%M%S"):
            try:
                return (1, dt.datetime.strptime(text, fmt))
            except ValueError:
                continue
        return (2, text.lower())

    def _sort_tree_by_column(self, tree: ttk.Treeview, column: str) -> None:
        sort_state = self.tree_sort_state.get(str(tree), {})
        ascending = sort_state.get(column, True)
        items = [(tree.set(k, column), k) for k in tree.get_children("")]
        items.sort(key=lambda pair: self._sort_key(pair[0]), reverse=not ascending)
        for index, (_, item_id) in enumerate(items):
            tree.move(item_id, "", index)
        sort_state[column] = not ascending
        self.tree_sort_state[str(tree)] = sort_state

    def _selected_success_sort_key(self) -> str:
        selected = self.success_sort_field.get().strip()
        for label, key in SUCCESS_SORT_FIELDS:
            if selected == label:
                return key
        return "run_ts"

    def _success_row_sort_key(self, row: dict, field: str):
        if field == "run_ts":
            parsed = self._parse_run_ts_to_dt(str(row.get("run_ts", "")))
            return parsed if parsed is not None else dt.datetime.min
        if field in {"id", "pre_content_length", "post_content_length", "content_delta"}:
            try:
                return int(str(row.get(field, "")).strip())
            except ValueError:
                return -1
        return str(row.get(field, "")).lower()

    def _emit(self, msg: str) -> None:
        self._append_file_log(msg)
        self.log_queue.put(msg)

    def _append_file_log(self, msg: str) -> None:
        with self.log_file_lock:
            with self.log_file_path.open("a", encoding="utf-8") as f:
                f.write(msg)

    def _append_history_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        self.history_file_path.parent.mkdir(parents=True, exist_ok=True)
        with self.history_file_lock:
            with self.history_file_path.open("a", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _ensure_pipeline_schema(self) -> None:
        conn = sqlite3.connect(str(self.pipeline_db_path))
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pipeline_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts TEXT NOT NULL,
                    doc_id INTEGER NOT NULL,
                    title TEXT NOT NULL DEFAULT '',
                    action TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    status TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    rag_md_path TEXT,
                    rag_json_path TEXT,
                    text_sha256 TEXT,
                    llm_provider TEXT,
                    llm_model TEXT,
                    paperless_update_status TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_pipeline_events_doc ON pipeline_events(doc_id);
                CREATE INDEX IF NOT EXISTS idx_pipeline_events_ts ON pipeline_events(event_ts);
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _record_pipeline_event(
        self,
        *,
        doc_id: int,
        title: str,
        action: str,
        engine: str,
        status: str,
        note: str = "",
        rag_md_path: str = "",
        rag_json_path: str = "",
        text_sha256: str = "",
        llm_provider: str = "",
        llm_model: str = "",
        paperless_update_status: str = "",
    ) -> None:
        conn = sqlite3.connect(str(self.pipeline_db_path))
        try:
            conn.execute(
                """
                INSERT INTO pipeline_events (
                    event_ts, doc_id, title, action, engine, status, note,
                    rag_md_path, rag_json_path, text_sha256,
                    llm_provider, llm_model, paperless_update_status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dt.datetime.now(dt.timezone.utc).isoformat(),
                    doc_id,
                    title,
                    action,
                    engine,
                    status,
                    note,
                    rag_md_path,
                    rag_json_path,
                    text_sha256,
                    llm_provider,
                    llm_model,
                    paperless_update_status,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _load_pipeline_events(self, limit: int = 1000) -> list[dict]:
        conn = sqlite3.connect(str(self.pipeline_db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT
                    event_ts, doc_id, title, action, engine, status, note,
                    rag_md_path, rag_json_path, paperless_update_status,
                    llm_provider, llm_model
                FROM pipeline_events
                ORDER BY event_ts DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def _drain_log_queue(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log.insert(END, msg)
                self.log.see(END)
                self._update_progress_from_log_line(msg)
        except queue.Empty:
            pass
        self.after(100, self._drain_log_queue)

    def _extract_id_from_line(self, line: str) -> int | None:
        marker = "ID="
        pos = line.find(marker)
        if pos < 0:
            return None
        value = []
        for ch in line[pos + len(marker) :]:
            if ch.isdigit():
                value.append(ch)
            else:
                break
        if not value:
            return None
        try:
            return int("".join(value))
        except ValueError:
            return None

    def _render_progress(self) -> None:
        total = max(self.run_total, 0)
        completed = len(self.run_completed_ids)
        pending = max(total - completed, 0)
        percent = (completed / total * 100.0) if total > 0 else 0.0
        self.progress_value.set(percent)
        self.progress_text.set(
            f"{self.progress_scope} | {completed}/{total} ({percent:.0f}%) | Pending: {pending}"
        )

    def _set_progress_scope(self, scope: str) -> None:
        self.progress_scope = scope
        self._render_progress()

    def _update_progress_from_log_line(self, line: str) -> None:
        if line.startswith("[START]"):
            doc_id = self._extract_id_from_line(line)
            if doc_id is not None:
                self.run_started_ids.add(doc_id)
                self._render_progress()
            return

        if line.startswith("[OK]") or line.startswith("[FAIL]"):
            doc_id = self._extract_id_from_line(line)
            if doc_id is not None:
                self.run_completed_ids.add(doc_id)
                self._render_progress()
            return

        if line.startswith("Summary:"):
            self._render_progress()

    def clear_log(self) -> None:
        self.log.delete("1.0", END)

    def _get_token(self) -> str:
        typed = self.api_token.get().strip()
        if typed:
            self._write_secret_file(DEFAULT_TOKEN_FILE, typed)
            return typed

        env_token = os.environ.get("PAPERLESS_API_TOKEN", "").strip()
        if env_token:
            return env_token

        return read_token_file(DEFAULT_TOKEN_FILE)

    def _safe_int(self, raw: str, field: str, minimum: int = 1) -> int:
        try:
            val = int(raw)
        except ValueError as exc:
            raise ValueError(f"{field} must be an integer") from exc
        if val < minimum:
            raise ValueError(f"{field} must be >= {minimum}")
        return val

    def _safe_optional_int(self, raw: str, field: str, minimum: int = 0) -> int | None:
        text = (raw or "").strip()
        if not text:
            return None
        return self._safe_int(text, field, minimum=minimum)

    def _api_headers(self, token: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": normalize_token_header(token),
        }

    def _api_post_json(
        self,
        url: str,
        headers: dict[str, str],
        payload: dict,
        verify_tls: bool,
        timeout: int,
    ) -> dict | list:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url=url, headers=headers, method="POST", data=body)
        context = None
        if not verify_tls:
            context = ssl._create_unverified_context()  # noqa: S323
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                raw = resp.read().decode("utf-8")
                if not raw.strip():
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error for {url}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"API returned non-JSON response for {url}") from exc

    def _api_patch_json(
        self,
        url: str,
        headers: dict[str, str],
        payload: dict,
        verify_tls: bool,
        timeout: int,
    ) -> dict | list:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url=url, headers=headers, method="PATCH", data=body)
        context = None
        if not verify_tls:
            context = ssl._create_unverified_context()  # noqa: S323
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                raw = resp.read().decode("utf-8")
                if not raw.strip():
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error for {url}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"API returned non-JSON response for {url}") from exc

    def _api_get_binary(
        self,
        url: str,
        headers: dict[str, str],
        verify_tls: bool,
        timeout: int,
    ) -> bytes:
        req = urllib.request.Request(url=url, headers=headers, method="GET")
        context = None
        if not verify_tls:
            context = ssl._create_unverified_context()  # noqa: S323
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error for {url}: {exc}") from exc

    def _fetch_document_raw_by_id(
        self,
        base_url: str,
        headers: dict[str, str],
        doc_id: int,
        timeout: int,
        verify_tls: bool,
    ) -> dict:
        payload = api_get_json(
            f"{base_url}/api/documents/{doc_id}/",
            headers=headers,
            verify_tls=verify_tls,
            timeout=timeout,
        )
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected document payload type for ID={doc_id}: {type(payload).__name__}")
        return payload

    def _download_document_pdf(
        self,
        base_url: str,
        headers: dict[str, str],
        doc_id: int,
        timeout: int,
        verify_tls: bool,
    ) -> bytes:
        # Paperless provides a download endpoint for original/archived document content.
        url = f"{base_url}/api/documents/{doc_id}/download/"
        return self._api_get_binary(
            url=url,
            headers=headers,
            verify_tls=verify_tls,
            timeout=timeout,
        )

    def _text_sha256(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _safe_engine_folder_name(self, engine: str) -> str:
        raw = (engine or "unknown").strip().lower()
        return re.sub(r"[^a-z0-9._-]+", "_", raw).strip("_") or "unknown"

    def _write_rag_export_files(
        self,
        *,
        doc_id: int,
        title: str,
        engine: str,
        text: str,
        metadata: dict,
    ) -> tuple[str, str]:
        export_root = Path(self.export_root_dir.get().strip() or str(self.rag_root_dir))
        engine_dir = export_root / self._safe_engine_folder_name(engine)
        doc_dir = engine_dir / str(doc_id)
        doc_dir.mkdir(parents=True, exist_ok=True)

        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        base = doc_dir / f"{ts}"
        md_path = Path(f"{base}.md")
        json_path = Path(f"{base}.json")
        export_mode = self.export_format_mode.get().strip().lower()

        payload = {
            "doc_id": doc_id,
            "title": title,
            "engine": engine,
            "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "text_sha256": self._text_sha256(text),
            "text": text,
            "metadata": metadata,
        }

        written_md = ""
        written_json = ""
        if export_mode in {"both", "md_only"}:
            md_lines = [
                f"# {title or f'Document {doc_id}'}",
                "",
                f"- doc_id: {doc_id}",
                f"- engine: {engine}",
                f"- exported_at: {payload['exported_at']}",
                "",
                text.strip(),
                "",
            ]
            md_path.write_text("\n".join(md_lines), encoding="utf-8")
            written_md = str(md_path)
        if export_mode in {"both", "json_only"}:
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            written_json = str(json_path)

        return written_md, written_json

    def _get_llm_api_key(self) -> str:
        typed = self.llm_api_key.get().strip()
        if typed:
            self._write_secret_file(DEFAULT_LLM_KEY_FILE, typed)
            return typed
        env_token = os.environ.get("OPENAI_API_KEY", "").strip()
        if env_token:
            return env_token
        key = read_token_file(DEFAULT_LLM_KEY_FILE)
        if key:
            return key
        return read_token_file(LEGACY_LLM_KEY_FILE)

    def _llm_headers(self, api_key: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

    def _llm_post_json(
        self,
        url: str,
        headers: dict[str, str],
        payload: dict,
        verify_tls: bool,
        timeout: int,
        retry_attempts: int = 0,
    ) -> dict | list:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url=url, headers=headers, method="POST", data=body)
        context = None
        if not verify_tls:
            context = ssl._create_unverified_context()  # noqa: S323
        attempts = max(1, retry_attempts + 1)
        for attempt in range(1, attempts + 1):
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                    raw = resp.read().decode("utf-8")
                    if not raw.strip():
                        return {}
                    return json.loads(raw)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                if 500 <= exc.code < 600 and attempt < attempts:
                    self._emit(
                        f"[WARN]  LLM API HTTP {exc.code} (attempt {attempt}/{attempts}), retrying...\n"
                    )
                    time.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
            except (TimeoutError, socket.timeout) as exc:
                if attempt < attempts:
                    self._emit(f"[WARN]  LLM API timeout (attempt {attempt}/{attempts}), retrying...\n")
                    time.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                raise RuntimeError(f"Network timeout for {url}: {exc}") from exc
            except urllib.error.URLError as exc:
                if attempt < attempts:
                    self._emit(
                        f"[WARN]  LLM API network error (attempt {attempt}/{attempts}): {exc}. Retrying...\n"
                    )
                    time.sleep(LLM_RETRY_BACKOFF_SECONDS * attempt)
                    continue
                raise RuntimeError(f"Network error for {url}: {exc}") from exc
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"LLM API returned non-JSON response for {url}") from exc
        raise RuntimeError(f"LLM API request exhausted retries for {url}")

    def _extract_llm_text(self, payload: dict | list) -> str:
        if isinstance(payload, dict):
            direct = payload.get("output_text")
            if isinstance(direct, str) and direct.strip():
                return direct.strip()
            # OpenAI-style responses API shape.
            output = payload.get("output")
            if isinstance(output, list):
                texts: list[str] = []
                for item in output:
                    if not isinstance(item, dict):
                        continue
                    contents = item.get("content")
                    if not isinstance(contents, list):
                        continue
                    for content in contents:
                        if not isinstance(content, dict):
                            continue
                        text_value = content.get("text")
                        if isinstance(text_value, str) and text_value.strip():
                            texts.append(text_value.strip())
                if texts:
                    return "\n\n".join(texts)
            # OpenAI-compatible chat completions shape.
            choices = payload.get("choices")
            if isinstance(choices, list) and choices:
                first = choices[0]
                if isinstance(first, dict):
                    message = first.get("message")
                    if isinstance(message, dict):
                        content = message.get("content")
                        if isinstance(content, str) and content.strip():
                            return content.strip()
        raise RuntimeError("Could not extract OCR text from LLM response payload")

    def _llm_ocr_pdf(
        self,
        *,
        pdf_bytes: bytes,
        filename: str,
        timeout: int,
        verify_tls: bool,
    ) -> str:
        api_key = self._get_llm_api_key()
        if not api_key:
            raise RuntimeError(
                "Missing LLM API key. Provide it in UI, set OPENAI_API_KEY, or put it in "
                f"{DEFAULT_LLM_KEY_FILE} (legacy fallback: {LEGACY_LLM_KEY_FILE})"
            )
        model = self.llm_model.get().strip()
        if not model:
            raise RuntimeError("LLM model is required")
        try:
            llm_timeout = self._safe_int(self.llm_timeout.get().strip(), "LLM Timeout", minimum=5)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        try:
            llm_retry_attempts = self._safe_int(
                self.llm_retry_attempts.get().strip(),
                "Retry attempts",
                minimum=0,
            )
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc
        effective_timeout = max(timeout, llm_timeout)

        llm_base = normalize_base_url(self.llm_api_base_url.get().strip())
        prompt = self.llm_prompt.get().strip()
        encoded_pdf = base64.b64encode(pdf_bytes).decode("ascii")
        file_data_url = f"data:application/pdf;base64,{encoded_pdf}"
        headers = self._llm_headers(api_key)

        mode = self.llm_mode.get().strip()
        if mode == LLM_MODE_CHAT:
            url = f"{llm_base}/v1/chat/completions"
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": "You are a high-fidelity OCR assistant."},
                    {
                        "role": "user",
                        "content": (
                            f"{prompt}\n\n"
                            f"Filename: {filename}\n"
                            f"PDF base64 data URL:\n{file_data_url}"
                        ),
                    },
                ],
                "temperature": 0,
            }
        else:
            url = f"{llm_base}/v1/responses"
            payload = {
                "model": model,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": prompt},
                            {
                                "type": "input_file",
                                "filename": filename,
                                "file_data": file_data_url,
                            },
                        ],
                    }
                ],
            }

        raw = self._llm_post_json(
            url=url,
            headers=headers,
            payload=payload,
            verify_tls=verify_tls,
            timeout=effective_timeout,
            retry_attempts=llm_retry_attempts,
        )
        return self._extract_llm_text(raw)

    def _iter_possible_task_ids(self, obj) -> list[str]:
        found: list[str] = []
        if obj is None:
            return found
        if isinstance(obj, str):
            candidate = obj.strip()
            if UUID_RE.match(candidate):
                return [candidate]
            return []
        if isinstance(obj, list):
            for item in obj:
                found.extend(self._iter_possible_task_ids(item))
            return found
        if isinstance(obj, dict):
            for key in ("task_id", "task_ids", "id", "task", "uuid"):
                if key in obj:
                    found.extend(self._iter_possible_task_ids(obj.get(key)))
            for value in obj.values():
                found.extend(self._iter_possible_task_ids(value))
            return found
        return found

    def _extract_task_ids(self, payload: dict | list) -> list[str]:
        dedup: list[str] = []
        seen: set[str] = set()
        for task_id in self._iter_possible_task_ids(payload):
            if task_id in seen:
                continue
            seen.add(task_id)
            dedup.append(task_id)
        return dedup

    def _task_state_from_payload(self, payload: dict | list) -> tuple[str, str]:
        task_obj = None
        if isinstance(payload, dict):
            results = payload.get("results")
            if isinstance(results, list) and results:
                if isinstance(results[0], dict):
                    task_obj = results[0]
            elif all(k in payload for k in ("id", "status")):
                task_obj = payload
        elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
            task_obj = payload[0]

        if not isinstance(task_obj, dict):
            return "PENDING", "Task metadata not available yet"

        for key in ("status", "state", "task_status"):
            value = task_obj.get(key)
            if isinstance(value, str) and value.strip():
                state = value.strip().upper()
                break
        else:
            state = "PENDING"

        detail_parts: list[str] = []
        for key in ("result", "message", "traceback"):
            value = task_obj.get(key)
            if isinstance(value, str) and value.strip():
                detail_parts.append(f"{key}={value.strip()}")
        detail = " | ".join(detail_parts)
        return state, detail

    def _classify_task_state(self, raw_state: str) -> str:
        state = (raw_state or "").upper()
        if state in {"SUCCESS", "SUCCEEDED", "DONE", "COMPLETED", "COMPLETE", "FINISHED"}:
            return "success"
        if state in {"FAILURE", "FAILED", "ERROR", "REVOKED", "CANCELED", "CANCELLED"}:
            return "failure"
        return "pending"

    def _poll_task_until_terminal(
        self,
        base_url: str,
        headers: dict[str, str],
        task_id: str,
        timeout: int,
        verify_tls: bool,
    ) -> tuple[str, str]:
        task_url = f"{base_url}/api/tasks/?task_id={urllib.parse.quote(task_id)}"
        while True:
            if self.stop_event.is_set():
                return "ABORTED", "Stopped by user"
            payload = api_get_json(task_url, headers=headers, verify_tls=verify_tls, timeout=timeout)
            state, detail = self._task_state_from_payload(payload)
            state_class = self._classify_task_state(state)
            if state_class == "success":
                return state, detail
            if state_class == "failure":
                return state, detail
            time.sleep(TASK_POLL_INTERVAL_SECONDS)

    def _extract_doc_snapshot(self, doc: dict) -> dict:
        return {
            "modified": doc.get("modified"),
            "content_length": int(doc.get("content_length") or 0),
            "archive_filename": str(doc.get("archive_filename") or ""),
            "page_count": doc.get("page_count"),
        }

    def _diff_snapshot(self, before: dict, after: dict) -> list[tuple[str, object, object]]:
        changed: list[tuple[str, object, object]] = []
        for key in ("modified", "content_length", "archive_filename", "page_count"):
            old = before.get(key)
            new = after.get(key)
            if old != new:
                changed.append((key, old, new))
        return changed

    def _fetch_document_by_id(
        self,
        base_url: str,
        headers: dict[str, str],
        doc_id: int,
        timeout: int,
        verify_tls: bool,
    ) -> dict:
        payload = self._fetch_document_raw_by_id(
            base_url=base_url,
            headers=headers,
            doc_id=doc_id,
            timeout=timeout,
            verify_tls=verify_tls,
        )
        return normalize_document(payload)

    def _poll_no_task_reprocess_diffs(
        self,
        base_url: str,
        headers: dict[str, str],
        baseline_snapshots: dict[int, dict],
        timeout: int,
        verify_tls: bool,
    ) -> tuple[set[int], set[int], set[int]]:
        pending = dict(baseline_snapshots)
        observed_ids: set[int] = set()
        no_observed_diff_ids: set[int] = set()
        stopped_ids: set[int] = set()
        start_ts = time.monotonic()

        if pending:
            self._emit(
                "[INFO] Starting heuristic diff polling for accepted reprocess jobs without task_id.\n"
            )

        while pending and not self.stop_event.is_set():
            elapsed = time.monotonic() - start_ts
            if elapsed >= NO_TASK_DIFF_MAX_WAIT_SECONDS:
                break

            for doc_id in list(pending.keys()):
                if self.stop_event.is_set():
                    break
                before = pending[doc_id]
                try:
                    current_doc = self._fetch_document_by_id(
                        base_url=base_url,
                        headers=headers,
                        doc_id=doc_id,
                        timeout=timeout,
                        verify_tls=verify_tls,
                    )
                except Exception as exc:
                    self._emit(f"[WARN]  ID={doc_id} (diff poll fetch error: {exc})\n")
                    continue

                after = self._extract_doc_snapshot(current_doc)
                changed_fields = self._diff_snapshot(before, after)
                if changed_fields:
                    observed_ids.add(doc_id)
                    changed_rendered = "; ".join(
                        f"{field} {old!s} -> {new!s}" for field, old, new in changed_fields
                    )
                    self._emit(
                        f"[OK]    ID={doc_id} "
                        f"(observed change via diff: {changed_rendered})\n"
                    )
                    pending.pop(doc_id, None)

            if pending and not self.stop_event.is_set():
                time.sleep(NO_TASK_DIFF_POLL_INTERVAL_SECONDS)

        if self.stop_event.is_set():
            for doc_id in list(pending.keys()):
                self._emit(f"[FAIL]  ID={doc_id} (stopped before diff observation)\n")
                stopped_ids.add(doc_id)
            return observed_ids, no_observed_diff_ids, stopped_ids

        for doc_id in list(pending.keys()):
            no_observed_diff_ids.add(doc_id)
            self._emit(
                f"[OK]    ID={doc_id} "
                "(accepted by API, no observable diff in wait window)\n"
            )
            pending.pop(doc_id, None)

        return observed_ids, no_observed_diff_ids, stopped_ids

    def refresh_all(self) -> None:
        try:
            page_size = self._safe_int(self.page_size.get().strip(), "Page Size")
            timeout = self._safe_int(self.timeout.get().strip(), "Timeout")
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        token = self._get_token()
        if not token:
            messagebox.showerror(
                "Missing token",
                f"No API token found. Enter one, set PAPERLESS_API_TOKEN, or place it in {DEFAULT_TOKEN_FILE}",
            )
            return

        self._emit("\n=== DATA REFRESH START ===\n")
        self.paperless_fetch_status.set("Paperless overview last fetched: fetching...")

        def worker() -> None:
            try:
                docs = fetch_all_documents(
                    api_base_url=self.api_base_url.get().strip(),
                    token=token,
                    page_size=page_size,
                    verify_tls=self.verify_tls.get(),
                    timeout=timeout,
                    progress_cb=lambda message: self._emit(message + "\n"),
                )
                success_rows, failed_rows = self._load_api_history_rows()
                recent_ids = self._recent_manual_ocr_ids(
                    rows=success_rows + failed_rows,
                    within_days=self._safe_int(self.recent_days.get().strip(), "Exclude days"),
                )

                self.docs = docs
                self.success_rows = success_rows
                self.failed_rows = failed_rows
                self.recent_manual_ids = recent_ids

                self._emit(f"Loaded docs={len(self.docs)}\n")
                self._emit(f"Loaded success_rows={len(self.success_rows)}\n")
                self._emit(f"Loaded failed_rows={len(self.failed_rows)}\n")

                self.after(0, self.refresh_candidates)
                self.after(0, self.refresh_success_tab)
                self.after(0, self.refresh_prospective)
                self.after(0, self.refresh_pdf_search)
                self.after(0, self.refresh_pipeline_overview)
                ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.after(0, self.paperless_fetch_status.set, f"Paperless overview last fetched: {ts}")
                self._emit("=== DATA REFRESH END ===\n")
            except Exception as exc:
                ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.after(0, self.paperless_fetch_status.set, f"Paperless overview fetch failed: {ts}")
                self._emit(f"[ERROR] Refresh failed: {exc}\n")

        threading.Thread(target=worker, daemon=True).start()

    def _load_api_history_rows(self) -> tuple[list[dict], list[dict]]:
        success_rows: list[dict] = []
        failed_rows: list[dict] = []
        if not self.history_file_path.exists():
            return success_rows, failed_rows

        with self.history_file_lock:
            with self.history_file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    row = {
                        "run_ts": str(payload.get("run_ts", "")),
                        "id": str(payload.get("id", "")),
                        "title": str(payload.get("title", "")),
                        "pre_content_length": str(payload.get("pre_content_length", "")),
                        "post_content_length": str(payload.get("post_content_length", "")),
                        "content_delta": str(payload.get("content_delta", "")),
                        "status": str(payload.get("status", "")),
                        "detail": str(payload.get("detail", "")),
                        "source_file": str(self.history_file_path),
                    }
                    if row["status"].lower() == "success":
                        success_rows.append(row)
                    else:
                        failed_rows.append(row)
        return success_rows, failed_rows

    def _parse_run_ts_to_dt(self, run_ts: str) -> dt.datetime | None:
        try:
            return dt.datetime.strptime(run_ts, "%Y-%m-%d_%H%M%S")
        except ValueError:
            return None

    def _recent_manual_ocr_ids(self, rows: list[dict], within_days: int) -> set[int]:
        cutoff = dt.datetime.now() - dt.timedelta(days=within_days)
        ids: set[int] = set()
        for row in rows:
            run_dt = self._parse_run_ts_to_dt(str(row.get("run_ts", "")))
            if run_dt is None or run_dt < cutoff:
                continue
            try:
                ids.add(int(str(row.get("id", "")).strip()))
            except ValueError:
                continue
        return ids

    def _last_manual_ocr_map(self, rows: list[dict]) -> dict[int, str]:
        last_map: dict[int, dt.datetime] = {}
        for row in rows:
            run_dt = self._parse_run_ts_to_dt(str(row.get("run_ts", "")))
            if run_dt is None:
                continue
            try:
                doc_id = int(str(row.get("id", "")).strip())
            except ValueError:
                continue
            previous = last_map.get(doc_id)
            if previous is None or run_dt > previous:
                last_map[doc_id] = run_dt
        return {doc_id: ts.strftime("%Y-%m-%d %H:%M:%S") for doc_id, ts in last_map.items()}

    def _set_run_candidates(self, candidates: list[dict], summary_text: str) -> None:
        self.selected_candidates = candidates
        last_map = self._last_manual_ocr_map(self.success_rows + self.failed_rows)
        rows = []
        for d in candidates:
            doc_id = int(d.get("id") or 0)
            rows.append(
                (
                    doc_id,
                    d.get("title") or "",
                    int(d.get("content_length") or 0),
                    d.get("modified") or "",
                    last_map.get(doc_id, "never"),
                )
            )
        self._fill_tree(self.run_tree, rows)
        self.run_summary.set(summary_text)

    def refresh_candidates(self) -> None:
        if not self.docs:
            self.run_summary.set("No documents loaded. Click 'Fetch overview from Paperless'.")
            self._fill_tree(self.run_tree, [])
            return

        try:
            batch_size = self._safe_int(self.batch_size.get().strip(), "Batch size")
            recent_days = self._safe_int(self.recent_days.get().strip(), "Exclude days", minimum=0)
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        recent_ids = self._recent_manual_ocr_ids(self.success_rows + self.failed_rows, within_days=recent_days)

        candidates = [d for d in self.docs if d.get("id") not in recent_ids]
        # Prioritize docs with smallest OCR text first.
        candidates.sort(key=lambda d: (int(d.get("content_length") or 0), int(d.get("id") or 0)))
        selected = candidates[:batch_size]
        self._set_run_candidates(
            selected,
            (
                "Candidates rebuilt from loaded overview: "
                f"{len(selected)} selected from {len(self.docs)} docs "
                f"(excluded recent manual OCR IDs: {len(recent_ids)})"
            )
        )

    def refresh_success_tab(self) -> None:
        sort_field = self._selected_success_sort_key()
        descending = self.success_sort_order.get() == SUCCESS_SORT_ORDERS[0]
        rows = []
        for row in sorted(
            self.success_rows,
            key=lambda r: self._success_row_sort_key(r, sort_field),
            reverse=descending,
        ):
            rows.append(
                (
                    row.get("run_ts", ""),
                    row.get("id", ""),
                    row.get("title", ""),
                    row.get("pre_content_length", ""),
                    row.get("post_content_length", ""),
                    row.get("content_delta", ""),
                    row.get("status", ""),
                )
            )
        self._fill_tree(self.success_tree, rows)
        self.success_summary.set(f"Successful OCR rows loaded: {len(rows)}")

    def refresh_success_history_only(self) -> None:
        try:
            success_rows, failed_rows = self._load_api_history_rows()
        except Exception as exc:
            messagebox.showerror("Refresh failed", f"Could not read API OCR archive: {exc}")
            return

        try:
            exclude_days = self._safe_int(self.recent_days.get().strip(), "Exclude days", minimum=0)
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        self.success_rows = success_rows
        self.failed_rows = failed_rows
        self.recent_manual_ids = self._recent_manual_ocr_ids(
            rows=self.success_rows + self.failed_rows,
            within_days=exclude_days,
        )
        self.refresh_success_tab()
        if self.docs:
            self.refresh_candidates()
            self.refresh_prospective()
            self.refresh_pdf_search()
        self.refresh_pipeline_overview()
        self._emit(
            f"[INFO] Refreshed API OCR archive only: success_rows={len(self.success_rows)} failed_rows={len(self.failed_rows)}\n"
        )

    def refresh_pipeline_overview(self) -> None:
        try:
            rows = self._load_pipeline_events(limit=2000)
        except Exception as exc:
            self.pipeline_rows = []
            self._fill_tree(self.pipeline_tree, [])
            self.pipeline_summary.set(f"Could not load pipeline overview: {exc}")
            return

        self.pipeline_rows = rows
        render_rows = []
        engine_counts: dict[str, int] = {}
        success_count = 0
        fail_count = 0
        for row in rows:
            engine = str(row.get("engine") or "")
            engine_counts[engine] = engine_counts.get(engine, 0) + 1
            status = str(row.get("status") or "")
            if status == "success":
                success_count += 1
            elif status == "failed":
                fail_count += 1
            render_rows.append(
                (
                    str(row.get("event_ts") or ""),
                    str(row.get("doc_id") or ""),
                    str(row.get("title") or ""),
                    str(row.get("action") or ""),
                    engine,
                    status,
                    str(row.get("paperless_update_status") or ""),
                    str(row.get("rag_md_path") or ""),
                    str(row.get("rag_json_path") or ""),
                )
            )
        self._fill_tree(self.pipeline_tree, render_rows)
        engine_summary = ", ".join(f"{k}:{v}" for k, v in sorted(engine_counts.items())) or "none"
        self.pipeline_summary.set(
            f"Events={len(rows)} success={success_count} failed={fail_count} | engines: {engine_summary}"
        )

    def refresh_prospective(self) -> None:
        if not self.docs:
            self.prospective_summary.set("No documents loaded. Click 'Fetch overview from Paperless'.")
            self._fill_tree(self.prospective_tree, [])
            return

        try:
            threshold = self._safe_int(self.prospective_threshold.get().strip(), "Prospective threshold", minimum=0)
            recent_days = self._safe_int(self.prospective_recent_days.get().strip(), "Prospective exclude days", minimum=0)
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        recent_ids = self._recent_manual_ocr_ids(self.success_rows + self.failed_rows, within_days=recent_days)
        last_map = self._last_manual_ocr_map(self.success_rows + self.failed_rows)

        prospective: list[dict] = []
        for d in self.docs:
            doc_id = int(d.get("id") or 0)
            if doc_id in recent_ids:
                continue

            reasons: list[str] = []
            content_length = int(d.get("content_length") or 0)
            if content_length < threshold:
                reasons.append(f"low_text<{threshold}")
            if not str(d.get("archive_filename") or "").strip():
                reasons.append("missing_archive")
            if str(d.get("mime_type") or "").lower() == "application/pdf" and content_length == 0:
                reasons.append("pdf_zero_text")

            if reasons:
                prospective.append(
                    {
                        "id": doc_id,
                        "title": d.get("title") or "",
                        "content_length": content_length,
                        "reason": ",".join(reasons),
                        "last_manual_ocr": last_map.get(doc_id, "never"),
                    }
                )

        prospective.sort(key=lambda r: (r["content_length"], r["id"]))
        self.prospective_rows = prospective

        rows = [(r["id"], r["title"], r["content_length"], r["reason"], r["last_manual_ocr"]) for r in prospective]
        self._fill_tree(self.prospective_tree, rows)
        self.prospective_summary.set(
            f"Prospective reruns: {len(prospective)} (threshold={threshold}, recent exclusion={recent_days} day(s))"
        )

    def _fill_tree(self, tree: ttk.Treeview, rows: list[tuple]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for row in rows:
            tree.insert("", END, values=row)

    def select_all_run_rows(self) -> None:
        items = self.run_tree.get_children("")
        if items:
            self.run_tree.selection_set(items)

    def clear_run_selection(self) -> None:
        selected_items = self.run_tree.selection()
        if selected_items:
            self.run_tree.selection_remove(selected_items)

    def _selected_run_doc_ids(self) -> list[int]:
        selected_ids: list[int] = []
        for item_id in self.run_tree.selection():
            values = self.run_tree.item(item_id, "values")
            if not values:
                continue
            try:
                selected_ids.append(int(values[0]))
            except (TypeError, ValueError):
                continue
        return selected_ids

    def _selected_pipeline_doc_ids(self) -> list[int]:
        selected_ids: list[int] = []
        for item_id in self.pipeline_tree.selection():
            values = self.pipeline_tree.item(item_id, "values")
            if not values:
                continue
            try:
                selected_ids.append(int(values[1]))
            except (TypeError, ValueError, IndexError):
                continue
        deduped: list[int] = []
        seen: set[int] = set()
        for doc_id in selected_ids:
            if doc_id in seen:
                continue
            seen.add(doc_id)
            deduped.append(doc_id)
        return deduped

    def _load_latest_llm_text(self, doc_id: int) -> tuple[str, str, dict] | None:
        conn = sqlite3.connect(str(self.pipeline_db_path))
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT rag_json_path, title
                FROM pipeline_events
                WHERE doc_id = ?
                  AND engine = ?
                  AND status = 'success'
                  AND action = 'llm_ocr'
                  AND rag_json_path IS NOT NULL
                  AND rag_json_path != ''
                ORDER BY event_ts DESC, id DESC
                LIMIT 1
                """,
                (doc_id, ENGINE_LLM),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        json_path = Path(str(row["rag_json_path"]))
        if not json_path.exists():
            return None
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        text = str(payload.get("text") or "")
        title = str(payload.get("title") or row["title"] or f"Document {doc_id}")
        metadata = dict(payload.get("metadata") or {})
        return text, title, metadata

    def _selected_ids_from_tree(self, tree: ttk.Treeview, id_col_index: int = 0) -> list[int]:
        ids: list[int] = []
        for item_id in tree.selection():
            values = tree.item(item_id, "values")
            if not values:
                continue
            try:
                ids.append(int(values[id_col_index]))
            except (TypeError, ValueError, IndexError):
                continue
        deduped: list[int] = []
        seen: set[int] = set()
        for doc_id in ids:
            if doc_id in seen:
                continue
            seen.add(doc_id)
            deduped.append(doc_id)
        return deduped

    def export_from_active_tab(self) -> None:
        selected_doc_ids: list[int] = []
        try:
            current_tab = self.nametowidget(self.notebook.select())
        except Exception:
            current_tab = self.tab_run

        if current_tab is self.tab_run:
            selected_doc_ids = self._selected_run_doc_ids()
        elif current_tab is self.tab_pipeline:
            selected_doc_ids = self._selected_pipeline_doc_ids()
        elif current_tab is self.tab_pdf_search:
            selected_doc_ids = self._selected_ids_from_tree(self.pdf_tree, id_col_index=0)
        elif current_tab is self.tab_prospective:
            selected_doc_ids = self._selected_ids_from_tree(self.prospective_tree, id_col_index=0)
        elif current_tab is self.tab_success:
            selected_doc_ids = self._selected_ids_from_tree(self.success_tree, id_col_index=1)
        else:
            selected_doc_ids = self._selected_run_doc_ids()

        if not selected_doc_ids:
            messagebox.showinfo(
                "No selection",
                (
                    "No selected document IDs found in the current tab.\n\n"
                    "Select rows in one of these tabs first:\n"
                    "- Pipeline Run\n"
                    "- Pipeline Overview\n"
                    "- Search Documents\n"
                    "- Prospective Reruns\n"
                    "- OCR History"
                ),
            )
            return
        self._export_documents_to_rag(selected_doc_ids)

    def export_pdf_search_selected_to_rag(self) -> None:
        selected_doc_ids = self._selected_ids_from_tree(self.pdf_tree, id_col_index=0)
        if not selected_doc_ids:
            messagebox.showinfo("No selection", "Select one or more rows in Search Documents first.")
            return
        self._export_documents_to_rag(selected_doc_ids)

    def export_selected_to_rag(self) -> None:
        if not self.docs:
            messagebox.showinfo("No data", "No documents loaded. Click 'Fetch overview from Paperless'.")
            return
        selected_doc_ids = self._selected_run_doc_ids()
        if not selected_doc_ids:
            messagebox.showinfo("No selection", "Select one or more rows in Pipeline Run first.")
            return
        self._export_documents_to_rag(selected_doc_ids)

    def export_pipeline_selected_to_rag(self) -> None:
        selected_doc_ids = self._selected_pipeline_doc_ids()
        if not selected_doc_ids:
            messagebox.showinfo("No selection", "Select one or more rows in Pipeline Overview first.")
            return
        self._export_documents_to_rag(selected_doc_ids)

    def _export_documents_to_rag(self, doc_ids: list[int]) -> None:
        if self.export_active:
            messagebox.showinfo("Export in progress", "A RAG export job is already running.")
            return
        if self.api_run_active:
            messagebox.showinfo("OCR in progress", "Wait for the active OCR run to finish before exporting.")
            return
        token = self._get_token()
        if not token:
            messagebox.showerror(
                "Missing token",
                f"No API token found. Enter one, set PAPERLESS_API_TOKEN, or place it in {DEFAULT_TOKEN_FILE}",
            )
            return
        try:
            timeout = self._safe_int(self.timeout.get().strip(), "Timeout")
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        source_mode = self.export_source_mode.get().strip() or ENGINE_PAPERLESS
        export_format = self.export_format_mode.get().strip() or "both"
        base_url = normalize_base_url(self.api_base_url.get().strip())
        headers = self._api_headers(token)
        verify_tls = self.verify_tls.get()

        self.run_total = len(doc_ids)
        self.run_completed_ids = set()
        self.run_started_ids = set()
        self._set_progress_scope("RAG Export")
        self._render_progress()
        self.stop_event.clear()
        self.export_active = True
        self._update_control_states()
        self._emit("\n=== RAG EXPORT QUEUED ===\n")
        self._emit(f"Source mode: {source_mode}\n")
        self._emit(f"Format: {export_format}\n")
        self._emit("IDs: " + ",".join(str(x) for x in doc_ids) + "\n")
        self.run_thread = threading.Thread(
            target=self._export_documents_to_rag_worker,
            args=(doc_ids, source_mode, export_format, base_url, headers, timeout, verify_tls),
            daemon=True,
        )
        self.run_thread.start()

    def _export_documents_to_rag_worker(
        self,
        doc_ids: list[int],
        source_mode: str,
        export_format: str,
        base_url: str,
        headers: dict[str, str],
        timeout: int,
        verify_tls: bool,
    ) -> None:
        self._emit(f"\n=== RAG EXPORT START source={source_mode} docs={len(doc_ids)} ===\n")
        ok_count = 0
        fail_count = 0
        try:
            for doc_id in doc_ids:
                if self.stop_event.is_set():
                    self._emit("[STOP] Export loop stopped by user\n")
                    break
                self._emit(f"[START] ID={doc_id}\n")
                title = f"Document {doc_id}"
                try:
                    if source_mode == ENGINE_LLM:
                        llm_data = self._load_latest_llm_text(doc_id)
                        if llm_data is None:
                            raise RuntimeError(
                                "No successful LLM OCR output found in local pipeline DB. "
                                "Run OCR with engine 'llm_openai_compatible' first, "
                                "or export using source 'paperless_internal'."
                            )
                        text, title, llm_meta = llm_data
                        metadata = {"source_mode": ENGINE_LLM, **llm_meta}
                        if not text.strip():
                            raise RuntimeError("Latest LLM OCR output exists but text is empty")
                    else:
                        raw_doc = self._fetch_document_raw_by_id(
                            base_url=base_url,
                            headers=headers,
                            doc_id=doc_id,
                            timeout=timeout,
                            verify_tls=verify_tls,
                        )
                        title = str(raw_doc.get("title") or title)
                        text = str(raw_doc.get("content") or "")
                        metadata = {
                            "source_mode": ENGINE_PAPERLESS,
                            "modified": raw_doc.get("modified"),
                            "mime_type": raw_doc.get("mime_type"),
                            "archive_filename": raw_doc.get("archive_filename") or raw_doc.get("archived_file_name"),
                            "original_filename": raw_doc.get("original_filename") or raw_doc.get("original_file_name"),
                        }
                        if not text.strip():
                            raise RuntimeError("Paperless content is empty for this document")

                    md_path, json_path = self._write_rag_export_files(
                        doc_id=doc_id,
                        title=title,
                        engine=source_mode,
                        text=text,
                        metadata=metadata,
                    )
                    self._record_pipeline_event(
                        doc_id=doc_id,
                        title=title,
                        action="rag_export",
                        engine=source_mode,
                        status="success",
                        note=f"export_format={export_format}",
                        rag_md_path=md_path,
                        rag_json_path=json_path,
                        text_sha256=self._text_sha256(text),
                    )
                    self._emit(
                        f"[OK]    ID={doc_id} exported to RAG "
                        f"(md={md_path or '-'}, json={json_path or '-'})\n"
                    )
                    ok_count += 1
                except Exception as exc:
                    self._record_pipeline_event(
                        doc_id=doc_id,
                        title=title,
                        action="rag_export",
                        engine=source_mode,
                        status="failed",
                        note=str(exc),
                    )
                    self._emit(f"[FAIL]  ID={doc_id} export failed: {exc}\n")
                    fail_count += 1
        except Exception as exc:
            self._emit(f"[ERROR] RAG export worker crashed: {exc}\n")
        finally:
            self._emit(f"Summary: rag_export success={ok_count} failed={fail_count} total={len(doc_ids)}\n")
            self._emit("=== RAG EXPORT END ===\n")
            self.export_active = False
            self.after(0, self._update_control_states)
            self.after(0, self.refresh_pipeline_overview)
            self.after(0, self._set_progress_scope, "Idle")
            self.after(0, self._render_progress)

    def _update_control_states(self) -> None:
        disabled = self.api_run_active or self.export_active
        if hasattr(self, "transfer_to_run_button"):
            self.transfer_to_run_button.configure(
                state=("disabled" if disabled else "normal")
            )
        if hasattr(self, "transfer_pdf_to_run_button"):
            self.transfer_pdf_to_run_button.configure(
                state=("disabled" if disabled else "normal")
            )

    def refresh_pdf_search(self) -> None:
        if not self.docs:
            self.pdf_summary.set("No documents loaded. Click 'Fetch overview from Paperless'.")
            self._fill_tree(self.pdf_tree, [])
            self.pdf_search_rows = []
            return

        try:
            min_chars = self._safe_optional_int(self.pdf_min_chars.get(), "Chars min", minimum=0)
            max_chars = self._safe_optional_int(self.pdf_max_chars.get(), "Chars max", minimum=0)
            min_pages = self._safe_optional_int(self.pdf_min_pages.get(), "Pages min", minimum=0)
            max_pages = self._safe_optional_int(self.pdf_max_pages.get(), "Pages max", minimum=0)
            exclude_recent_days = self._safe_optional_int(
                self.pdf_exclude_recent_days.get(), "Exclude OCR last days", minimum=0
            )
            exclude_recent_days = 0 if exclude_recent_days is None else exclude_recent_days
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        if min_chars is not None and max_chars is not None and max_chars < min_chars:
            messagebox.showerror("Invalid input", "Chars max must be >= chars min")
            return
        if min_pages is not None and max_pages is not None and max_pages < min_pages:
            messagebox.showerror("Invalid input", "Pages max must be >= pages min")
            return

        query = self.pdf_query.get().strip().lower()
        modified_contains = self.pdf_modified_contains.get().strip().lower()
        missing_archive_only = self.pdf_missing_archive_only.get()

        history_rows = self.success_rows + self.failed_rows
        recent_ids = (
            self._recent_manual_ocr_ids(history_rows, within_days=exclude_recent_days)
            if exclude_recent_days > 0
            else set()
        )

        docs_to_search = list(self.docs)
        filtered: list[dict] = []

        for d in docs_to_search:
            doc_id = int(d.get("id") or 0)
            if recent_ids and doc_id in recent_ids:
                continue

            title = str(d.get("title") or "")
            archive_filename = str(d.get("archive_filename") or "")
            original_filename = str(d.get("original_filename") or "")
            modified = str(d.get("modified") or "")
            content_length = int(d.get("content_length") or 0)

            page_count_raw = d.get("page_count")
            if page_count_raw is None:
                page_count = None
            else:
                try:
                    page_count = int(page_count_raw)
                except (TypeError, ValueError):
                    page_count = None

            if query:
                haystack = " ".join(
                    [
                        str(doc_id),
                        title,
                        archive_filename,
                        original_filename,
                        modified,
                    ]
                ).lower()
                if query not in haystack:
                    continue

            if modified_contains and modified_contains not in modified.lower():
                continue
            if missing_archive_only and archive_filename.strip():
                continue
            if min_chars is not None and content_length < min_chars:
                continue
            if max_chars is not None and content_length > max_chars:
                continue
            if min_pages is not None and (page_count is None or page_count < min_pages):
                continue
            if max_pages is not None and (page_count is None or page_count > max_pages):
                continue

            filtered.append(
                {
                    "id": doc_id,
                    "title": title,
                    "content_length": content_length,
                    "page_count": page_count,
                    "modified": modified,
                    "archive_filename": archive_filename,
                    "original_filename": original_filename,
                }
            )

        filtered.sort(key=lambda row: (row["content_length"], row["id"]))
        self.pdf_search_rows = filtered

        rows = [
            (
                row["id"],
                row["title"],
                row["content_length"],
                row["page_count"] if row["page_count"] is not None else "",
                row["modified"],
                row["archive_filename"],
                row["original_filename"],
            )
            for row in filtered
        ]
        self._fill_tree(self.pdf_tree, rows)
        self.pdf_summary.set(
            "Search results: "
            f"{len(filtered)} of {len(docs_to_search)} documents "
            f"(exclude_recent_days={exclude_recent_days})"
        )

    def reset_pdf_search_filters(self) -> None:
        self.pdf_query.set("")
        self.pdf_modified_contains.set("")
        self.pdf_missing_archive_only.set(False)
        self.pdf_exclude_recent_days.set("0")
        self.pdf_min_chars.set("")
        self.pdf_max_chars.set("")
        self.pdf_min_pages.set("")
        self.pdf_max_pages.set("")
        self.refresh_pdf_search()

    def transfer_pdf_search_to_run(self) -> None:
        if self.api_run_active:
            messagebox.showinfo(
                "Run in progress",
                "Transfer is disabled while an OCR job is running.",
            )
            return
        if not self.docs:
            messagebox.showinfo("No data", "No documents loaded. Click 'Fetch overview from Paperless'.")
            return

        selected_items = self.pdf_tree.selection()
        if not selected_items:
            messagebox.showinfo(
                "No selection",
                "Select one or more rows in Search Documents first.",
            )
            return

        selected_ids: list[int] = []
        for item_id in selected_items:
            values = self.pdf_tree.item(item_id, "values")
            if not values:
                continue
            try:
                selected_ids.append(int(values[0]))
            except (TypeError, ValueError):
                continue

        if not selected_ids:
            messagebox.showinfo("No valid IDs", "Could not parse selected document IDs.")
            return

        docs_by_id = {int(d["id"]): d for d in self.pdf_search_rows}
        transfer_docs: list[dict] = []
        for doc_id in selected_ids:
            doc = docs_by_id.get(doc_id)
            if doc is not None:
                transfer_docs.append(doc)

        if not transfer_docs:
            messagebox.showinfo("Not found", "Selected IDs were not found in current Search Documents result.")
            return

        self._set_run_candidates(
            transfer_docs,
            f"Transferred {len(transfer_docs)} document(s) from Search Documents.",
        )
        self.notebook.select(self.tab_run)
        self._emit(
            f"[INFO] Transferred to Run OCR from Search Documents: ids={','.join(str(d['id']) for d in transfer_docs)}\n"
        )

    def transfer_prospective_to_run(self) -> None:
        if self.api_run_active:
            messagebox.showinfo(
                "Run in progress",
                "Transfer is disabled while an OCR job is running.",
            )
            return
        if not self.docs:
            messagebox.showinfo("No data", "No documents loaded. Click 'Fetch overview from Paperless'.")
            return

        selected_items = self.prospective_tree.selection()
        if not selected_items:
            messagebox.showinfo(
                "No selection",
                "Select one or more rows in Prospective Reruns first.",
            )
            return

        selected_ids: list[int] = []
        for item_id in selected_items:
            values = self.prospective_tree.item(item_id, "values")
            if not values:
                continue
            try:
                selected_ids.append(int(values[0]))
            except (TypeError, ValueError):
                continue

        if not selected_ids:
            messagebox.showinfo("No valid IDs", "Could not parse selected document IDs.")
            return

        doc_by_id = {int(d.get("id") or 0): d for d in self.docs}
        transfer_docs: list[dict] = []
        for doc_id in selected_ids:
            doc = doc_by_id.get(doc_id)
            if doc is not None:
                transfer_docs.append(doc)

        if not transfer_docs:
            messagebox.showinfo("Not found", "Selected IDs were not found in loaded documents.")
            return

        self._set_run_candidates(
            transfer_docs,
            f"Transferred {len(transfer_docs)} document(s) from Prospective Reruns.",
        )
        self.notebook.select(self.tab_run)
        self._emit(
            f"[INFO] Transferred to Run OCR from Prospective Reruns: ids={','.join(str(d['id']) for d in transfer_docs)}\n"
        )

    def run_selected_ocr(self) -> None:
        if not self.selected_candidates:
            messagebox.showinfo("No candidates", "No selected candidates. Build candidate list first.")
            return
        if self.export_active:
            messagebox.showinfo("Export in progress", "A RAG export job is active. Wait for it to finish first.")
            return
        if self.api_run_active:
            messagebox.showinfo("Run in progress", "An OCR run is already active.")
            return

        selected_doc_ids = self._selected_run_doc_ids()
        if not selected_doc_ids:
            messagebox.showinfo(
                "No selection",
                "Mark one or more rows in Run OCR first. Use Select All to run the full list.",
            )
            return

        try:
            timeout = self._safe_int(self.timeout.get().strip(), "Timeout")
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        token = self._get_token()
        if not token:
            messagebox.showerror(
                "Missing token",
                f"No API token found. Enter one, set PAPERLESS_API_TOKEN, or place it in {DEFAULT_TOKEN_FILE}",
            )
            return

        selected_docs_by_id = {int(d.get("id") or 0): d for d in self.selected_candidates}
        run_docs: list[dict] = []
        missing_doc_ids: list[int] = []
        for doc_id in selected_doc_ids:
            doc = selected_docs_by_id.get(doc_id)
            if doc is None:
                missing_doc_ids.append(doc_id)
                continue
            run_docs.append(doc)

        if not run_docs:
            messagebox.showerror(
                "Selection error",
                "Selected rows could not be resolved to loaded candidates. Rebuild candidates and try again.",
            )
            return
        if missing_doc_ids:
            self._emit(
                "[WARN] Ignoring selected IDs not present in current candidate set: "
                + ",".join(str(x) for x in missing_doc_ids)
                + "\n"
            )

        engine_mode = self.ocr_engine_mode.get().strip() or ENGINE_PAPERLESS
        ids = [str(int(d.get("id") or 0)) for d in run_docs]
        ids_csv = ",".join(ids)
        baseline_snapshots = {
            int(d.get("id") or 0): self._extract_doc_snapshot(d) for d in run_docs
        }
        baseline_docs = {int(d.get("id") or 0): d for d in run_docs}
        run_ts = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        self.run_total = len(ids)
        self.run_completed_ids = set()
        self.run_started_ids = set()
        if engine_mode == ENGINE_LLM:
            self._set_progress_scope("LLM OCR")
        else:
            self._set_progress_scope("Paperless OCR")
        self._render_progress()
        self.stop_event.clear()
        self.api_run_active = True
        self._update_control_states()

        base_url = normalize_base_url(self.api_base_url.get().strip())
        headers = self._api_headers(token)
        self._emit("\n=== OCR RUN START ===\n")
        self._emit(f"Engine mode: {engine_mode}\n")
        self._emit("IDs: " + ids_csv + "\n")

        if engine_mode == ENGINE_LLM:
            self.run_thread = threading.Thread(
                target=self._run_llm_ocr_worker,
                args=(
                    base_url,
                    headers,
                    [int(x) for x in ids],
                    baseline_docs,
                    run_ts,
                    timeout,
                    self.verify_tls.get(),
                ),
                daemon=True,
            )
        else:
            self._emit(f"API mode: POST {base_url}/api/documents/bulk_edit/ (docs={len(ids)})\n")
            self.run_thread = threading.Thread(
                target=self._run_api_reprocess_worker,
                args=(
                    base_url,
                    headers,
                    [int(x) for x in ids],
                    baseline_docs,
                    baseline_snapshots,
                    run_ts,
                    timeout,
                    self.verify_tls.get(),
                ),
                daemon=True,
            )
        self.run_thread.start()

    def _run_llm_ocr_worker(
        self,
        base_url: str,
        headers: dict[str, str],
        doc_ids: list[int],
        baseline_docs: dict[int, dict],
        run_ts: str,
        timeout: int,
        verify_tls: bool,
    ) -> None:
        archive_rows: list[dict] = []
        success_count = 0
        fail_count = 0
        try:
            for doc_id in doc_ids:
                if self.stop_event.is_set():
                    self._emit("[STOP] LLM OCR loop stopped by user\n")
                    break
                self._emit(f"[START] ID={doc_id}\n")
                title = str(baseline_docs.get(doc_id, {}).get("title") or f"Document {doc_id}")
                detail = ""
                status = "failed"
                post_len = 0
                text = ""
                md_path = ""
                json_path = ""
                paperless_update_status = ""
                try:
                    raw_doc = self._fetch_document_raw_by_id(
                        base_url=base_url,
                        headers=headers,
                        doc_id=doc_id,
                        timeout=timeout,
                        verify_tls=verify_tls,
                    )
                    title = str(raw_doc.get("title") or title)
                    filename = str(
                        raw_doc.get("archive_filename")
                        or raw_doc.get("archived_file_name")
                        or raw_doc.get("original_filename")
                        or raw_doc.get("original_file_name")
                        or f"{doc_id}.pdf"
                    )
                    pdf_bytes = self._download_document_pdf(
                        base_url=base_url,
                        headers=headers,
                        doc_id=doc_id,
                        timeout=timeout,
                        verify_tls=verify_tls,
                    )
                    self._emit(
                        f"[INFO]  ID={doc_id} sending PDF to LLM API "
                        f"(bytes={len(pdf_bytes)}, mode={self.llm_mode.get().strip() or LLM_MODE_RESPONSES}, "
                        f"timeout={self.llm_timeout.get().strip() or DEFAULT_LLM_TIMEOUT_SECONDS}s, "
                        f"retries={self.llm_retry_attempts.get().strip() or DEFAULT_LLM_RETRY_ATTEMPTS})\n"
                    )
                    text = self._llm_ocr_pdf(
                        pdf_bytes=pdf_bytes,
                        filename=filename,
                        timeout=timeout,
                        verify_tls=verify_tls,
                    )
                    post_len = len(text)
                    md_path, json_path = self._write_rag_export_files(
                        doc_id=doc_id,
                        title=title,
                        engine=ENGINE_LLM,
                        text=text,
                        metadata={
                            "source_mode": ENGINE_LLM,
                            "run_ts": run_ts,
                            "filename": filename,
                        },
                    )
                    if self.llm_update_paperless.get():
                        try:
                            self._api_patch_json(
                                url=f"{base_url}/api/documents/{doc_id}/",
                                headers=headers,
                                payload={"content": text},
                                verify_tls=verify_tls,
                                timeout=timeout,
                            )
                            paperless_update_status = "updated"
                        except Exception as exc:
                            paperless_update_status = f"failed:{exc}"

                    status = "success"
                    detail = "llm_ocr_completed"
                    self._emit(f"[OK]    ID={doc_id} (LLM OCR text_len={post_len})\n")
                    success_count += 1
                except Exception as exc:
                    detail = str(exc)
                    self._emit(f"[FAIL]  ID={doc_id} (LLM OCR error: {exc})\n")
                    fail_count += 1

                self._record_pipeline_event(
                    doc_id=doc_id,
                    title=title,
                    action="llm_ocr",
                    engine=ENGINE_LLM,
                    status=status,
                    note=detail,
                    rag_md_path=md_path,
                    rag_json_path=json_path,
                    text_sha256=self._text_sha256(text) if text else "",
                    llm_provider=normalize_base_url(self.llm_api_base_url.get().strip()),
                    llm_model=self.llm_model.get().strip(),
                    paperless_update_status=paperless_update_status,
                )

                pre_len = int(baseline_docs.get(doc_id, {}).get("content_length") or 0)
                archive_rows.append(
                    {
                        "run_ts": run_ts,
                        "id": doc_id,
                        "title": title,
                        "pre_content_length": pre_len,
                        "post_content_length": post_len,
                        "content_delta": post_len - pre_len,
                        "status": status,
                        "detail": detail,
                        "source": ENGINE_LLM,
                    }
                )
        except Exception as exc:
            self._emit(f"[ERROR] LLM OCR worker crashed: {exc}\n")
        finally:
            self.api_run_active = False
            self.after(0, self._update_control_states)
            self._emit(f"Summary: success={success_count} failed={fail_count} total={len(doc_ids)}\n")
            self._emit("=== OCR RUN END (llm) ===\n")
            if archive_rows:
                self._append_history_rows(archive_rows)
                self.success_rows, self.failed_rows = self._load_api_history_rows()
                self.after(0, self.refresh_success_tab)
            self.after(0, self.refresh_pipeline_overview)
            self.after(0, self._set_progress_scope, "Idle")
            self.after(0, self._render_progress)

    def _run_api_reprocess_worker(
        self,
        base_url: str,
        headers: dict[str, str],
        doc_ids: list[int],
        baseline_docs: dict[int, dict],
        baseline_snapshots: dict[int, dict],
        run_ts: str,
        timeout: int,
        verify_tls: bool,
    ) -> None:
        accepted_no_task_count = 0
        accepted_no_task_observed = 0
        accepted_no_task_no_diff = 0
        submitted_tasks: list[tuple[int, str]] = []
        no_task_baselines: dict[int, dict] = {}
        emitted_no_task_hint = False
        doc_results: dict[int, dict[str, str]] = {
            doc_id: {"status": "pending", "detail": "not_submitted"} for doc_id in doc_ids
        }
        archive_rows: list[dict] = []
        success_count = 0
        fail_count = 0

        try:
            for doc_id in doc_ids:
                if self.stop_event.is_set():
                    self._emit("[STOP] Submission loop stopped by user\n")
                    break

                self._emit(f"[START] ID={doc_id}\n")
                try:
                    payload = self._api_post_json(
                        url=f"{base_url}/api/documents/bulk_edit/",
                        headers=headers,
                        payload={"documents": [doc_id], "method": "reprocess"},
                        verify_tls=verify_tls,
                        timeout=timeout,
                    )
                    task_ids = self._extract_task_ids(payload)
                    if not task_ids:
                        # Paperless 2.20.x commonly returns {"result":"OK"} without task IDs for bulk reprocess.
                        result_value = ""
                        if isinstance(payload, dict):
                            result_value = str(payload.get("result", "")).strip().upper()
                        if result_value == "OK":
                            accepted_no_task_count += 1
                            no_task_baselines[doc_id] = baseline_snapshots.get(doc_id, {})
                            doc_results[doc_id] = {
                                "status": "pending",
                                "detail": "accepted_by_api_no_task_id",
                            }
                            if not emitted_no_task_hint:
                                self._emit(
                                    "[INFO] API returned no task_id for reprocess. "
                                    "This server version accepts jobs but does not expose per-job poll IDs.\n"
                                )
                                emitted_no_task_hint = True
                            self._emit(f"[INFO]  ID={doc_id} (accepted by API, queued for diff observation)\n")
                        else:
                            doc_results[doc_id] = {
                                "status": "failed",
                                "detail": f"no_task_id_payload={payload}",
                            }
                            self._emit(
                                f"[FAIL]  ID={doc_id} "
                                "(no task_id returned from API, payload={payload})\n"
                            )
                        continue
                    if len(task_ids) > 1:
                        self._emit(
                            f"[WARN]  ID={doc_id} API returned multiple task_ids; tracking first only: {','.join(task_ids)}\n"
                        )
                    submitted_tasks.append((doc_id, task_ids[0]))
                    doc_results[doc_id] = {"status": "pending", "detail": f"task_id={task_ids[0]}"}
                    self._emit(f"[TASK]  ID={doc_id} task_id={task_ids[0]}\n")
                except Exception as exc:
                    doc_results[doc_id] = {"status": "failed", "detail": f"submit_error={exc}"}
                    self._emit(f"[FAIL]  ID={doc_id} (submit error: {exc})\n")

            for doc_id, task_id in submitted_tasks:
                if self.stop_event.is_set():
                    self._emit("[STOP] Poll loop stopped by user\n")
                    break
                state, detail = self._poll_task_until_terminal(
                    base_url=base_url,
                    headers=headers,
                    task_id=task_id,
                    timeout=timeout,
                    verify_tls=verify_tls,
                )
                if self.stop_event.is_set():
                    break
                if self._classify_task_state(state) == "success":
                    detail_text = f"task_state={state}"
                    if detail:
                        detail_text += f", {detail}"
                    doc_results[doc_id] = {"status": "success", "detail": detail_text}
                    self._emit(f"[OK]    ID={doc_id}\n")
                else:
                    suffix = f", detail={detail}" if detail else ""
                    doc_results[doc_id] = {
                        "status": "failed",
                        "detail": f"task_state={state}{suffix}",
                    }
                    self._emit(f"[FAIL]  ID={doc_id} (task_state={state}{suffix})\n")

            if no_task_baselines:
                observed_ids, no_diff_ids, stopped_ids = self._poll_no_task_reprocess_diffs(
                    base_url=base_url,
                    headers=headers,
                    baseline_snapshots=no_task_baselines,
                    timeout=timeout,
                    verify_tls=verify_tls,
                )
                accepted_no_task_observed = len(observed_ids)
                accepted_no_task_no_diff = len(no_diff_ids)
                for doc_id in observed_ids:
                    doc_results[doc_id] = {"status": "success", "detail": "observed_change_via_diff"}
                for doc_id in no_diff_ids:
                    doc_results[doc_id] = {"status": "success", "detail": "accepted_no_observed_diff"}
                for doc_id in stopped_ids:
                    doc_results[doc_id] = {"status": "failed", "detail": "stopped_before_diff_observation"}

            for doc_id in doc_ids:
                state = doc_results.get(doc_id, {}).get("status", "pending")
                if state != "pending":
                    continue
                if self.stop_event.is_set():
                    doc_results[doc_id] = {"status": "failed", "detail": "stopped_before_completion"}
                    self._emit(f"[FAIL]  ID={doc_id} (stopped before completion)\n")
                else:
                    doc_results[doc_id] = {"status": "failed", "detail": "incomplete_without_terminal_status"}
                    self._emit(f"[FAIL]  ID={doc_id} (incomplete without terminal status)\n")

            for doc_id in doc_ids:
                pre_len = int(baseline_snapshots.get(doc_id, {}).get("content_length") or 0)
                base_title = str(baseline_docs.get(doc_id, {}).get("title") or "")
                title = base_title
                post_len = pre_len
                detail = doc_results.get(doc_id, {}).get("detail", "")
                status = doc_results.get(doc_id, {}).get("status", "failed")

                try:
                    latest_doc = self._fetch_document_by_id(
                        base_url=base_url,
                        headers=headers,
                        doc_id=doc_id,
                        timeout=timeout,
                        verify_tls=verify_tls,
                    )
                    title = str(latest_doc.get("title") or title)
                    post_len = int(latest_doc.get("content_length") or 0)
                except Exception as exc:
                    if detail:
                        detail += f" | post_fetch_error={exc}"
                    else:
                        detail = f"post_fetch_error={exc}"
                    self._emit(f"[WARN]  ID={doc_id} (post-run snapshot fetch failed: {exc})\n")

                archive_rows.append(
                    {
                        "run_ts": run_ts,
                        "id": doc_id,
                        "title": title,
                        "pre_content_length": pre_len,
                        "post_content_length": post_len,
                        "content_delta": post_len - pre_len,
                        "status": "success" if status == "success" else "failed",
                        "detail": detail,
                        "source": "api_bulk_reprocess",
                    }
                )
                self._record_pipeline_event(
                    doc_id=doc_id,
                    title=title,
                    action="paperless_reprocess",
                    engine=ENGINE_PAPERLESS,
                    status="success" if status == "success" else "failed",
                    note=detail,
                )

            success_count = sum(1 for row in archive_rows if row.get("status") == "success")
            fail_count = len(archive_rows) - success_count
            self._append_history_rows(archive_rows)
        except Exception as exc:
            self._emit(f"[ERROR] API OCR worker crashed: {exc}\n")
        finally:
            self.api_run_active = False
            self.after(0, self._update_control_states)
            self._emit(f"Summary: success={success_count} failed={fail_count} total={len(doc_ids)}\n")
            if accepted_no_task_count:
                self._emit(
                    "Summary detail: "
                    f"accepted_without_task_id={accepted_no_task_count} "
                    f"observed_diff={accepted_no_task_observed} "
                    f"no_observed_diff={accepted_no_task_no_diff}\n"
                )
            if archive_rows:
                self._emit(
                    f"[INFO] Appended API OCR archive rows={len(archive_rows)} file={self.history_file_path}\n"
                )
            self._emit("=== OCR RUN END (api) ===\n")
            self.success_rows, self.failed_rows = self._load_api_history_rows()
            self.after(0, self.refresh_success_tab)
            self.after(0, self.refresh_pipeline_overview)
            self.after(0, self._set_progress_scope, "Idle")
            self.after(0, self._render_progress)

    def stop_run(self) -> None:
        if not self.api_run_active and not self.export_active:
            self._emit("No active run to stop.\n")
            return
        self.stop_event.set()
        if self.api_run_active:
            self._emit("Stop requested. Current API task poll/submission will stop shortly.\n")
        if self.export_active:
            self._emit("Stop requested. Current export job will stop shortly.\n")
        self._render_progress()

    def _on_window_close(self) -> None:
        if self._settings_autosave_after_id is not None:
            try:
                self.after_cancel(self._settings_autosave_after_id)
            except Exception:
                pass
            self._settings_autosave_after_id = None
        self._save_settings(show_error=False)
        self.destroy()


if __name__ == "__main__":
    app = OcrDashboard()
    app.mainloop()
