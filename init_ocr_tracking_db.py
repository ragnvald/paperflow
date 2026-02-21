#!/usr/bin/env python3
import argparse
import datetime as dt
import hashlib
import json
import os
import sqlite3
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_DB_DIR = SCRIPT_DIR / "data_memory"
DEFAULT_DB_PATH = DEFAULT_DB_DIR / "paperless_ocr_tracking.sqlite3"
DEFAULT_API_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_TOKEN_FILE = SCRIPT_DIR.parent / "secrets" / "paperlesstoken.api"

RUN_TYPES = ("bootstrap", "sync", "ocr-rerun")


def normalize_base_url(value: str) -> str:
    return value.rstrip("/")


def resolve_db_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return DEFAULT_DB_DIR / path


def parse_ocr_engines(values: list[str]) -> list[str]:
    engines: list[str] = []
    for value in values:
        parts = [part.strip() for part in value.split(",") if part.strip()]
        engines.extend(parts)
    unique: list[str] = []
    seen: set[str] = set()
    for engine in engines:
        if engine in seen:
            continue
        seen.add(engine)
        unique.append(engine)
    return unique


def normalize_token_header(token: str) -> str:
    stripped = token.strip()
    if stripped.lower().startswith("token ") or stripped.lower().startswith("bearer "):
        return stripped
    return f"Token {stripped}"


def read_token_file(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def api_get_json(url: str, headers: dict[str, str], verify_tls: bool, timeout: int) -> dict | list:
    req = urllib.request.Request(url=url, headers=headers, method="GET")
    context = None
    if not verify_tls:
        context = ssl._create_unverified_context()  # noqa: S323
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Network error for {url}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"API returned non-JSON response for {url}") from exc


def run_preflight(
    api_base_url: str,
    token: str,
    page_size: int,
    verify_tls: bool,
    timeout: int,
) -> tuple[bool, str]:
    headers = {
        "Accept": "application/json",
        "Authorization": normalize_token_header(token),
    }
    url = f"{normalize_base_url(api_base_url)}/api/documents/?page=1&page_size={page_size}"
    try:
        payload = api_get_json(url, headers=headers, verify_tls=verify_tls, timeout=timeout)
    except Exception as exc:
        return False, str(exc)

    if isinstance(payload, dict) and "results" in payload:
        count = payload.get("count")
        return True, f"API reachable (documents_count={count})"
    if isinstance(payload, list):
        return True, "API reachable (non-paginated list response)"
    return False, f"Unexpected response type: {type(payload).__name__}"


def first_present(doc: dict, keys: tuple[str, ...], default=None):
    for key in keys:
        if key in doc and doc[key] is not None:
            return doc[key]
    return default


def normalize_document(raw: dict) -> dict:
    doc_id = first_present(raw, ("id",))
    if doc_id is None:
        raise ValueError("Document payload missing 'id'")

    original_filename = first_present(
        raw,
        (
            "original_filename",
            "original_file_name",
            "original_file",
            "filename",
        ),
        "",
    )
    archive_filename = first_present(
        raw,
        (
            "archive_filename",
            "archived_file_name",
            "archive_file_name",
            "archive_file",
        ),
        "",
    )
    modified = first_present(raw, ("modified", "updated", "created"), None)
    content = first_present(raw, ("content", "text"), "")

    content_length_raw = first_present(raw, ("content_length",), None)
    if content_length_raw is None:
        content_length = len(content or "")
    else:
        try:
            content_length = int(content_length_raw)
        except (TypeError, ValueError):
            content_length = len(content or "")

    page_count_raw = first_present(raw, ("page_count", "pages"), None)
    try:
        page_count = int(page_count_raw) if page_count_raw is not None else None
    except (TypeError, ValueError):
        page_count = None

    return {
        "id": int(doc_id),
        "title": str(first_present(raw, ("title",), "") or ""),
        "mime_type": str(first_present(raw, ("mime_type", "mime"), "") or ""),
        "original_filename": str(original_filename or ""),
        "archive_filename": str(archive_filename or ""),
        "page_count": page_count,
        "modified": str(modified) if modified else None,
        "content_length": content_length,
    }


def fetch_all_documents(
    api_base_url: str,
    token: str,
    page_size: int,
    verify_tls: bool,
    timeout: int,
    progress_cb=None,
) -> list[dict]:
    headers = {
        "Accept": "application/json",
        "Authorization": normalize_token_header(token),
    }

    base = normalize_base_url(api_base_url)
    next_url = f"{base}/api/documents/?page=1&page_size={page_size}"
    docs: list[dict] = []
    page_no = 0

    while next_url:
        page_no += 1
        payload = api_get_json(next_url, headers=headers, verify_tls=verify_tls, timeout=timeout)

        if isinstance(payload, dict):
            results = payload.get("results")
            if not isinstance(results, list):
                raise RuntimeError(
                    "Unexpected paginated response shape from /api/documents/ (missing list 'results')"
                )
            for item in results:
                if not isinstance(item, dict):
                    continue
                docs.append(normalize_document(item))

            next_value = payload.get("next")
            if isinstance(next_value, str) and next_value.strip():
                next_url = urllib.parse.urljoin(base + "/", next_value)
            else:
                next_url = ""
        elif isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                docs.append(normalize_document(item))
            next_url = ""
        else:
            raise RuntimeError(f"Unexpected response type from /api/documents/: {type(payload).__name__}")

        message = f"Fetched page {page_no}: total_docs_so_far={len(docs)}"
        print(message)
        if progress_cb is not None:
            progress_cb(message)

    docs.sort(key=lambda d: d["id"])
    return docs


def stable_fingerprint(doc: dict) -> str:
    payload = {
        "title": doc.get("title") or "",
        "mime_type": doc.get("mime_type") or "",
        "original_filename": doc.get("original_filename") or "",
        "archive_filename": doc.get("archive_filename") or "",
        "page_count": doc.get("page_count"),
        "modified": doc.get("modified"),
        "content_length": int(doc.get("content_length") or 0),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def detect_changed_fields(previous: sqlite3.Row, current: dict) -> list[str]:
    fields = [
        "title",
        "mime_type",
        "original_filename",
        "archive_filename",
        "page_count",
        "modified",
        "content_length",
    ]
    changed: list[str] = []
    for field in fields:
        old_value = previous[field]
        new_value = current.get(field)
        if old_value != new_value:
            changed.append(field)
    return changed


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            run_type TEXT NOT NULL,
            manage_root TEXT NOT NULL DEFAULT '',
            manage_python TEXT NOT NULL DEFAULT '',
            exec_mode TEXT NOT NULL DEFAULT '',
            api_base_url TEXT NOT NULL DEFAULT '',
            auth_mode TEXT NOT NULL DEFAULT 'token',
            ocr_engines_json TEXT,
            ocr_provider TEXT,
            ocr_model TEXT,
            notes TEXT,
            total_documents INTEGER NOT NULL DEFAULT 0,
            new_documents INTEGER NOT NULL DEFAULT 0,
            changed_documents INTEGER NOT NULL DEFAULT 0,
            unchanged_documents INTEGER NOT NULL DEFAULT 0,
            missing_documents INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS tracked_documents (
            paperless_id INTEGER PRIMARY KEY,
            first_seen_run_id INTEGER NOT NULL,
            last_seen_run_id INTEGER NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            deleted_at TEXT,
            deleted_in_run_id INTEGER,
            title TEXT NOT NULL DEFAULT '',
            mime_type TEXT NOT NULL DEFAULT '',
            original_filename TEXT NOT NULL DEFAULT '',
            archive_filename TEXT NOT NULL DEFAULT '',
            page_count INTEGER,
            modified TEXT,
            content_length INTEGER NOT NULL DEFAULT 0,
            current_fingerprint TEXT NOT NULL,
            FOREIGN KEY(first_seen_run_id) REFERENCES runs(id),
            FOREIGN KEY(last_seen_run_id) REFERENCES runs(id),
            FOREIGN KEY(deleted_in_run_id) REFERENCES runs(id)
        );

        CREATE TABLE IF NOT EXISTS document_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            paperless_id INTEGER NOT NULL,
            observed_at TEXT NOT NULL,
            classification TEXT NOT NULL,
            changed_fields_json TEXT,
            previous_fingerprint TEXT,
            new_fingerprint TEXT,
            title TEXT NOT NULL DEFAULT '',
            mime_type TEXT NOT NULL DEFAULT '',
            original_filename TEXT NOT NULL DEFAULT '',
            archive_filename TEXT NOT NULL DEFAULT '',
            page_count INTEGER,
            modified TEXT,
            content_length INTEGER NOT NULL DEFAULT 0,
            UNIQUE(run_id, paperless_id),
            FOREIGN KEY(run_id) REFERENCES runs(id),
            FOREIGN KEY(paperless_id) REFERENCES tracked_documents(paperless_id)
        );

        CREATE INDEX IF NOT EXISTS idx_classifications_doc_id ON document_classifications(paperless_id);
        CREATE INDEX IF NOT EXISTS idx_classifications_run_id ON document_classifications(run_id);
        """
    )


def ensure_schema_evolution(conn: sqlite3.Connection) -> None:
    run_columns = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    if "api_base_url" not in run_columns:
        conn.execute("ALTER TABLE runs ADD COLUMN api_base_url TEXT NOT NULL DEFAULT ''")
    if "auth_mode" not in run_columns:
        conn.execute("ALTER TABLE runs ADD COLUMN auth_mode TEXT NOT NULL DEFAULT 'token'")

    tracked_columns = {row[1] for row in conn.execute("PRAGMA table_info(tracked_documents)").fetchall()}
    if "deleted_at" not in tracked_columns:
        conn.execute("ALTER TABLE tracked_documents ADD COLUMN deleted_at TEXT")
    if "deleted_in_run_id" not in tracked_columns:
        conn.execute("ALTER TABLE tracked_documents ADD COLUMN deleted_in_run_id INTEGER")


def mark_missing_documents(conn: sqlite3.Connection, run_id: int, observed_ids: set[int], now_iso: str) -> int:
    cursor = conn.execute("SELECT paperless_id FROM tracked_documents WHERE is_active = 1")
    active_ids = {int(row[0]) for row in cursor.fetchall()}
    missing_ids = sorted(active_ids - observed_ids)
    for paperless_id in missing_ids:
        conn.execute(
            """
            UPDATE tracked_documents
            SET is_active = 0
              , deleted_at = ?
              , deleted_in_run_id = ?
            WHERE paperless_id = ?
            """,
            (now_iso, run_id, paperless_id),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO document_classifications (
                run_id, paperless_id, observed_at, classification, changed_fields_json,
                previous_fingerprint, new_fingerprint, title, mime_type, original_filename,
                archive_filename, page_count, modified, content_length
            )
            VALUES (?, ?, ?, 'missing', '[]', NULL, NULL, '', '', '', '', NULL, NULL, 0)
            """,
            (run_id, paperless_id, now_iso),
        )
    return len(missing_ids)


def run_sync(
    conn: sqlite3.Connection,
    docs: list[dict],
    run_id: int,
    observed_at: str,
) -> tuple[int, int, int]:
    new_count = 0
    changed_count = 0
    unchanged_count = 0

    for doc in docs:
        doc_id = doc["id"]
        fingerprint = stable_fingerprint(doc)
        row = conn.execute(
            "SELECT * FROM tracked_documents WHERE paperless_id = ?",
            (doc_id,),
        ).fetchone()
        if row is None:
            new_count += 1
            conn.execute(
                """
                INSERT INTO tracked_documents (
                    paperless_id, first_seen_run_id, last_seen_run_id, first_seen_at, last_seen_at, is_active,
                    deleted_at, deleted_in_run_id,
                    title, mime_type, original_filename, archive_filename, page_count, modified,
                    content_length, current_fingerprint
                )
                VALUES (?, ?, ?, ?, ?, 1, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc_id,
                    run_id,
                    run_id,
                    observed_at,
                    observed_at,
                    doc["title"],
                    doc["mime_type"],
                    doc["original_filename"],
                    doc["archive_filename"],
                    doc["page_count"],
                    doc["modified"],
                    doc["content_length"],
                    fingerprint,
                ),
            )
            conn.execute(
                """
                INSERT INTO document_classifications (
                    run_id, paperless_id, observed_at, classification, changed_fields_json,
                    previous_fingerprint, new_fingerprint, title, mime_type, original_filename,
                    archive_filename, page_count, modified, content_length
                )
                VALUES (?, ?, ?, 'new', ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    doc_id,
                    observed_at,
                    json.dumps(
                        [
                            "title",
                            "mime_type",
                            "original_filename",
                            "archive_filename",
                            "page_count",
                            "modified",
                            "content_length",
                        ]
                    ),
                    fingerprint,
                    doc["title"],
                    doc["mime_type"],
                    doc["original_filename"],
                    doc["archive_filename"],
                    doc["page_count"],
                    doc["modified"],
                    doc["content_length"],
                ),
            )
            continue

        previous_fingerprint = row["current_fingerprint"]
        changed_fields = detect_changed_fields(row, doc)
        if previous_fingerprint != fingerprint:
            changed_count += 1
            classification = "changed"
        else:
            unchanged_count += 1
            classification = "unchanged"

        conn.execute(
            """
            UPDATE tracked_documents
            SET
                last_seen_run_id = ?,
                last_seen_at = ?,
                is_active = 1,
                deleted_at = NULL,
                deleted_in_run_id = NULL,
                title = ?,
                mime_type = ?,
                original_filename = ?,
                archive_filename = ?,
                page_count = ?,
                modified = ?,
                content_length = ?,
                current_fingerprint = ?
            WHERE paperless_id = ?
            """,
            (
                run_id,
                observed_at,
                doc["title"],
                doc["mime_type"],
                doc["original_filename"],
                doc["archive_filename"],
                doc["page_count"],
                doc["modified"],
                doc["content_length"],
                fingerprint,
                doc_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO document_classifications (
                run_id, paperless_id, observed_at, classification, changed_fields_json,
                previous_fingerprint, new_fingerprint, title, mime_type, original_filename,
                archive_filename, page_count, modified, content_length
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                doc_id,
                observed_at,
                classification,
                json.dumps(changed_fields),
                previous_fingerprint,
                fingerprint,
                doc["title"],
                doc["mime_type"],
                doc["original_filename"],
                doc["archive_filename"],
                doc["page_count"],
                doc["modified"],
                doc["content_length"],
            ),
        )

    return new_count, changed_count, unchanged_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Initialize/sync a separate OCR tracking database from Paperless documents API. "
            "Safe to run repeatedly."
        )
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="SQLite DB file path")
    parser.add_argument(
        "--run-type",
        default="sync",
        choices=RUN_TYPES,
        help="Run classification for auditing (default: sync)",
    )
    parser.add_argument(
        "--ocr-engine",
        action="append",
        default=[],
        help="OCR engine name(s), repeatable or comma-separated (e.g. tesseract, mistral-ocr)",
    )
    parser.add_argument("--ocr-provider", default=None, help="Optional OCR provider (e.g. local, openai)")
    parser.add_argument("--ocr-model", default=None, help="Optional OCR model name/version")
    parser.add_argument("--notes", default=None, help="Optional note for this run")
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help=f"Paperless base URL (default: {DEFAULT_API_BASE_URL})",
    )
    parser.add_argument(
        "--api-token",
        default="",
        help="Paperless API token (or set PAPERLESS_API_TOKEN env var)",
    )
    parser.add_argument(
        "--api-token-file",
        default=str(DEFAULT_TOKEN_FILE),
        help=f"Path to API token file (default: {DEFAULT_TOKEN_FILE})",
    )
    parser.add_argument("--page-size", type=int, default=200, help="API page size for /api/documents/")
    parser.add_argument(
        "--verify-tls",
        action="store_true",
        help="Verify TLS certificates (default: disabled)",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")

    args = parser.parse_args()

    token_file = Path(args.api_token_file)
    token = (
        args.api_token.strip()
        or os.environ.get("PAPERLESS_API_TOKEN", "").strip()
        or read_token_file(token_file)
    )
    if not token:
        print(
            "Missing API token. Pass --api-token, set PAPERLESS_API_TOKEN, "
            f"or place it in {token_file}.",
            file=sys.stderr,
        )
        return 2
    if args.page_size < 1:
        print("--page-size must be >= 1", file=sys.stderr)
        return 2
    if args.timeout < 1:
        print("--timeout must be >= 1", file=sys.stderr)
        return 2

    ok, message = run_preflight(
        api_base_url=args.api_base_url,
        token=token,
        page_size=min(args.page_size, 5),
        verify_tls=args.verify_tls,
        timeout=args.timeout,
    )
    if not ok:
        print(f"Preflight failed: {message}", file=sys.stderr)
        return 1
    print(f"Preflight: {message}")

    docs = fetch_all_documents(
        api_base_url=args.api_base_url,
        token=token,
        page_size=args.page_size,
        verify_tls=args.verify_tls,
        timeout=args.timeout,
    )
    print(f"Loaded {len(docs)} document(s) from Paperless API")

    db_path = resolve_db_path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        create_schema(conn)
        ensure_schema_evolution(conn)
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        ocr_engines = parse_ocr_engines(args.ocr_engine)
        conn.execute(
            """
            INSERT INTO runs (
                started_at, run_type,
                manage_root, manage_python, exec_mode,
                api_base_url, auth_mode,
                ocr_engines_json, ocr_provider, ocr_model, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_iso,
                args.run_type,
                "",
                "",
                "api",
                normalize_base_url(args.api_base_url),
                "token",
                json.dumps(ocr_engines),
                args.ocr_provider,
                args.ocr_model,
                args.notes,
            ),
        )
        run_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        new_count, changed_count, unchanged_count = run_sync(
            conn=conn,
            docs=docs,
            run_id=run_id,
            observed_at=now_iso,
        )
        missing_count = mark_missing_documents(
            conn=conn,
            run_id=run_id,
            observed_ids={d["id"] for d in docs},
            now_iso=now_iso,
        )

        conn.execute(
            """
            UPDATE runs
            SET
                completed_at = ?,
                total_documents = ?,
                new_documents = ?,
                changed_documents = ?,
                unchanged_documents = ?,
                missing_documents = ?
            WHERE id = ?
            """,
            (
                dt.datetime.now(dt.timezone.utc).isoformat(),
                len(docs),
                new_count,
                changed_count,
                unchanged_count,
                missing_count,
                run_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Tracking DB: {db_path}")
    print(
        "Run summary: "
        f"run_id={run_id} type={args.run_type} total={len(docs)} "
        f"new={new_count} changed={changed_count} unchanged={unchanged_count} missing={missing_count}"
    )
    if ocr_engines:
        print(f"OCR engines: {', '.join(ocr_engines)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
