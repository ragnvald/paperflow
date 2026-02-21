#!/usr/bin/env python3
import datetime as dt
import json
import os
import queue
import re
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

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_OUT_DIR = SCRIPT_DIR / "data_out"
API_OCR_HISTORY_PATH = DATA_OUT_DIR / "api_ocr_history.jsonl"

BATCH_OPTIONS = tuple([str(i) for i in range(5, 101, 5)] + ["250", "500", "1000"])
TASK_POLL_INTERVAL_SECONDS = 2.0
NO_TASK_DIFF_POLL_INTERVAL_SECONDS = 5.0
NO_TASK_DIFF_MAX_WAIT_SECONDS = 600.0
UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class OcrDashboard(tb.Window):
    def __init__(self) -> None:
        super().__init__(themename="flatly")
        self.title("Paperless OCR Control Center")
        self.geometry("1300x860")

        self.run_thread: threading.Thread | None = None
        self.api_run_active = False
        self.stop_event = threading.Event()
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.log_file_lock = threading.Lock()
        self.history_file_lock = threading.Lock()
        self.log_file_path = DATA_OUT_DIR / "log.txt"
        self.log_file_path.parent.mkdir(parents=True, exist_ok=True)
        self.history_file_path = API_OCR_HISTORY_PATH

        self.docs: list[dict] = []
        self.recent_manual_ids: set[int] = set()
        self.success_rows: list[dict] = []
        self.failed_rows: list[dict] = []

        self.selected_candidates: list[dict] = []
        self.low_text_rows: list[dict] = []
        self.prospective_rows: list[dict] = []
        self.run_total = 0
        self.run_completed_ids: set[int] = set()
        self.run_started_ids: set[int] = set()
        self.tree_sort_state: dict[str, dict[str, bool]] = {}

        self._build_ui()
        self._append_file_log(f"\n===== DASHBOARD START {dt.datetime.now().isoformat()} =====\n")
        self.after(100, self._drain_log_queue)

    def _build_ui(self) -> None:
        root = tb.Frame(self, padding=10)
        root.pack(fill=BOTH, expand=True)

        self.top_notebook = ttk.Notebook(root)
        self.top_notebook.pack(fill=X, pady=(0, 8))

        self.top_general_tab = tb.Frame(self.top_notebook, padding=8)
        self.top_log_tab = tb.Frame(self.top_notebook, padding=8)
        self.top_notebook.add(self.top_general_tab, text="General Settings")
        self.top_notebook.add(self.top_log_tab, text="Log")

        self._build_top_controls(self.top_general_tab)
        self.log = ScrolledText(self.top_log_tab, wrap="word", height=10)
        self.log.pack(fill=BOTH, expand=True)

        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=BOTH, expand=True)

        self.tab_run = tb.Frame(self.notebook, padding=8)
        self.tab_low = tb.Frame(self.notebook, padding=8)
        self.tab_success = tb.Frame(self.notebook, padding=8)
        self.tab_prospective = tb.Frame(self.notebook, padding=8)

        self.notebook.add(self.tab_run, text="Run OCR")
        self.notebook.add(self.tab_low, text="Suspicious Low Text")
        self.notebook.add(self.tab_success, text="Successful OCR")
        self.notebook.add(self.tab_prospective, text="Prospective Reruns")

        self._build_run_tab()
        self._build_low_text_tab()
        self._build_success_tab()
        self._build_prospective_tab()

    def _build_top_controls(self, parent: tk.Widget) -> None:
        top = tb.Labelframe(parent, text="API + General Settings", padding=8)
        top.pack(fill=X)

        self.api_base_url = tk.StringVar(value=DEFAULT_API_BASE_URL)
        self.api_token = tk.StringVar(value="")
        self.verify_tls = tk.BooleanVar(value=False)
        self.page_size = tk.StringVar(value="200")
        self.timeout = tk.StringVar(value="30")

        self._add_form_row(top, 0, "API Base URL", self.api_base_url)
        self._add_form_row(top, 1, "API Token (optional)", self.api_token, show="*")
        self._add_form_row(top, 2, "Page Size", self.page_size)
        self._add_form_row(top, 3, "Timeout (s)", self.timeout)

        tb.Checkbutton(
            top,
            text="Verify TLS",
            variable=self.verify_tls,
            bootstyle="round-toggle",
        ).grid(row=4, column=1, sticky=W, padx=6, pady=4)

        top.columnconfigure(1, weight=1)

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

    def _build_run_tab(self) -> None:
        controls = tb.Frame(self.tab_run)
        controls.pack(fill=X)

        self.batch_size = tk.StringVar(value="50")
        self.recent_days = tk.StringVar(value="14")

        tb.Label(controls, text="Batch size").pack(side=LEFT, padx=(0, 6))
        tb.Combobox(controls, values=BATCH_OPTIONS, textvariable=self.batch_size, state="readonly", width=8).pack(
            side=LEFT, padx=(0, 12)
        )

        tb.Label(controls, text="Exclude OCR in last days").pack(side=LEFT, padx=(0, 6))
        tb.Entry(controls, textvariable=self.recent_days, width=8).pack(side=LEFT, padx=(0, 12))

        run_actions = tb.Frame(self.tab_run)
        run_actions.pack(fill=X, pady=(8, 6))

        data_actions = tb.Labelframe(run_actions, text="Data", padding=6)
        data_actions.pack(side=LEFT, padx=(0, 10))
        tb.Button(data_actions, text="Refresh All Data", bootstyle="primary", command=self.refresh_all).pack(
            side=LEFT, padx=(0, 8)
        )
        tb.Button(data_actions, text="Refresh Candidates", bootstyle="info", command=self.refresh_candidates).pack(
            side=LEFT, padx=(0, 8)
        )

        exec_actions = tb.Labelframe(run_actions, text="Run", padding=6)
        exec_actions.pack(side=LEFT)
        tb.Button(exec_actions, text="Run Selected OCR", bootstyle="success", command=self.run_selected_ocr).pack(
            side=LEFT, padx=(0, 8)
        )
        tb.Button(exec_actions, text="Stop OCR", bootstyle="danger", command=self.stop_run).pack(
            side=LEFT, padx=(0, 8)
        )
        tb.Button(exec_actions, text="Clear Log", bootstyle="secondary", command=self.clear_log).pack(
            side=LEFT, padx=(0, 2)
        )

        selection_actions = tb.Labelframe(run_actions, text="Selection", padding=6)
        selection_actions.pack(side=LEFT, padx=(10, 0))
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
        ).pack(side=LEFT)

        self.run_summary = tk.StringVar(value="No candidate set built yet")
        tb.Label(self.tab_run, textvariable=self.run_summary, bootstyle="secondary").pack(anchor=W, pady=(8, 6))
        tb.Label(
            self.tab_run,
            text="OCR-kjøring skjer via Paperless API (bulk reprocess + task polling).",
            bootstyle="info",
        ).pack(anchor=W, pady=(0, 6))
        self.progress_text = tk.StringVar(value="Fremdrift: 0/0 (0%) | På vent: 0")
        tb.Label(self.tab_run, textvariable=self.progress_text, bootstyle="info").pack(anchor=W, pady=(0, 4))
        self.progress_value = tk.DoubleVar(value=0.0)
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

    def _build_low_text_tab(self) -> None:
        controls = tb.Frame(self.tab_low)
        controls.pack(fill=X)

        self.low_threshold = tk.StringVar(value="100")
        tb.Label(controls, text="Suspicious if chars <").pack(side=LEFT, padx=(0, 6))
        tb.Entry(controls, textvariable=self.low_threshold, width=8).pack(side=LEFT, padx=(0, 12))
        tb.Button(controls, text="Refresh Low Text", command=self.refresh_low_text, bootstyle="info").pack(side=LEFT)

        self.low_summary = tk.StringVar(value="No data loaded")
        tb.Label(self.tab_low, textvariable=self.low_summary, bootstyle="secondary").pack(anchor=W, pady=(8, 6))

        self.low_tree = self._build_tree(
            self.tab_low,
            columns=("id", "title", "content_length", "page_count", "modified"),
            headings=("ID", "Title", "Chars", "Pages", "Modified"),
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
                anchor="e",
                command=lambda c=col, t=tree: self._sort_tree_by_column(t, c),
            )
            width = 120
            if col in ("title", "reason"):
                width = 420
            elif col in ("modified", "last_manual_ocr", "run_ts"):
                width = 180
            tree.column(col, width=width, anchor="e")

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
        self.progress_text.set(f"Fremdrift: {completed}/{total} ({percent:.0f}%) | På vent: {pending}")

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
        payload = api_get_json(
            f"{base_url}/api/documents/{doc_id}/",
            headers=headers,
            verify_tls=verify_tls,
            timeout=timeout,
        )
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected document payload type for ID={doc_id}: {type(payload).__name__}")
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
                self.after(0, self.refresh_low_text)
                self.after(0, self.refresh_success_tab)
                self.after(0, self.refresh_prospective)
                self._emit("=== DATA REFRESH END ===\n")
            except Exception as exc:
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
            self.run_summary.set("No documents loaded. Click Refresh All Data.")
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
            f"Candidates built: {len(selected)} selected from {len(self.docs)} docs (excluded recent manual OCR IDs: {len(recent_ids)})"
        )

    def refresh_low_text(self) -> None:
        if not self.docs:
            self.low_summary.set("No documents loaded. Click Refresh All Data.")
            self._fill_tree(self.low_tree, [])
            return

        try:
            threshold = self._safe_int(self.low_threshold.get().strip(), "Low-text threshold", minimum=0)
        except ValueError as exc:
            messagebox.showerror("Invalid input", str(exc))
            return

        low = [d for d in self.docs if int(d.get("content_length") or 0) < threshold]
        low.sort(key=lambda d: (int(d.get("content_length") or 0), int(d.get("id") or 0)))
        self.low_text_rows = low

        rows = [
            (
                int(d.get("id") or 0),
                d.get("title") or "",
                int(d.get("content_length") or 0),
                d.get("page_count") if d.get("page_count") is not None else "",
                d.get("modified") or "",
            )
            for d in low
        ]
        self._fill_tree(self.low_tree, rows)
        self.low_summary.set(f"Suspicious low-text objects: {len(low)} (threshold={threshold})")

    def refresh_success_tab(self) -> None:
        rows = []
        for row in sorted(self.success_rows, key=lambda r: str(r.get("run_ts", "")), reverse=True):
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
        self._emit(
            f"[INFO] Refreshed API OCR archive only: success_rows={len(self.success_rows)} failed_rows={len(self.failed_rows)}\n"
        )

    def refresh_prospective(self) -> None:
        if not self.docs:
            self.prospective_summary.set("No documents loaded. Click Refresh All Data.")
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

    def _update_control_states(self) -> None:
        if hasattr(self, "transfer_to_run_button"):
            self.transfer_to_run_button.configure(
                state=("disabled" if self.api_run_active else "normal")
            )

    def transfer_prospective_to_run(self) -> None:
        if self.api_run_active:
            messagebox.showinfo(
                "Run in progress",
                "Transfer is disabled while an OCR job is running.",
            )
            return
        if not self.docs:
            messagebox.showinfo("No data", "No documents loaded. Click Refresh All Data.")
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
                "Selected rows could not be resolved to loaded candidates. Refresh candidates and try again.",
            )
            return
        if missing_doc_ids:
            self._emit(
                "[WARN] Ignoring selected IDs not present in current candidate set: "
                + ",".join(str(x) for x in missing_doc_ids)
                + "\n"
            )

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
        self._render_progress()
        self.stop_event.clear()
        self.api_run_active = True
        self._update_control_states()

        base_url = normalize_base_url(self.api_base_url.get().strip())
        headers = self._api_headers(token)
        self._emit("\n=== OCR RUN START ===\n")
        self._emit(f"API mode: POST {base_url}/api/documents/bulk_edit/ (docs={len(ids)})\n")
        self._emit("IDs: " + ids_csv + "\n")

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
            self._render_progress()

    def stop_run(self) -> None:
        if not self.api_run_active:
            self._emit("No active run to stop.\n")
            return
        self.stop_event.set()
        self._emit("Stop requested. Current API task poll/submission will stop shortly.\n")
        self._render_progress()


if __name__ == "__main__":
    app = OcrDashboard()
    app.mainloop()
