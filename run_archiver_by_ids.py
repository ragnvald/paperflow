#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import html
import json
import os
import pwd
import random
import shlex
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

MANAGE_ROOT = "/opt/paperless/src"
MANAGE_PYTHON = "/opt/paperless/venv/bin/python"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "data_out"
EXEC_MODE_AUTO = "auto"
EXEC_MODE_SUDO = "sudo"
EXEC_MODE_DIRECT = "direct"
VALID_EXEC_MODES = (EXEC_MODE_AUTO, EXEC_MODE_SUDO, EXEC_MODE_DIRECT)
RESULT_FIELDS = [
    "id",
    "title",
    "mime_type",
    "original_filename",
    "archive_filename",
    "status",
    "exit_code",
    "partial_progress",
    "pre_content_length",
    "post_content_length",
    "content_delta",
    "pre_page_count",
    "post_page_count",
    "archive_changed",
    "modified_changed",
    "error",
]


def build_manage_cmd(manage_root: str, manage_python: str, args: str) -> str:
    return f"cd {shlex.quote(manage_root)} && {shlex.quote(manage_python)} manage.py {args}"


def parse_json_from_mixed_output(raw: str) -> dict | list:
    text = (raw or "").strip()
    if not text:
        raise json.JSONDecodeError("empty output", raw, 0)

    for line in reversed(text.splitlines()):
        candidate = line.strip()
        if not candidate:
            continue
        if not (candidate.startswith("{") or candidate.startswith("[")):
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    return json.loads(text)


def run_manage_py(
    args: str,
    manage_root: str,
    manage_python: str,
    exec_mode: str,
    check: bool = True,
) -> subprocess.CompletedProcess:
    manage_cmd = shlex.quote(build_manage_cmd(manage_root, manage_python, args))
    current_user = pwd.getpwuid(os.geteuid()).pw_name
    if exec_mode == EXEC_MODE_DIRECT:
        cmd = f"bash -lc {manage_cmd}"
    elif exec_mode == EXEC_MODE_SUDO:
        if current_user == "paperless":
            cmd = f"bash -lc {manage_cmd}"
        elif os.geteuid() == 0:
            cmd = f"runuser -u paperless -- bash -lc {manage_cmd}"
        else:
            cmd = f"sudo -n -u paperless bash -lc {manage_cmd}"
    else:
        if current_user == "paperless":
            cmd = f"bash -lc {manage_cmd}"
        elif os.geteuid() == 0:
            cmd = f"runuser -u paperless -- bash -lc {manage_cmd}"
        else:
            cmd = f"bash -lc {manage_cmd}"
    return subprocess.run(cmd, shell=True, text=True, capture_output=True, check=check)


def parse_ids_csv(ids_csv: str) -> list[int]:
    parts = [segment.strip() for segment in ids_csv.split(",") if segment.strip()]
    ids: list[int] = []
    for part in parts:
        if not part.isdigit():
            raise ValueError(f"Invalid ID in --ids: {part}")
        ids.append(int(part))
    return ids


def get_ids(
    source_mode: str,
    ids_csv: str | None,
    manage_root: str,
    manage_python: str,
    exec_mode: str,
) -> list[int]:
    if source_mode == "ids":
        if not ids_csv:
            raise ValueError("--ids requires a comma-separated list of numeric IDs")
        return parse_ids_csv(ids_csv)

    if source_mode == "all":
        code = (
            "import json;"
            "from documents.models import Document;"
            "print(json.dumps(list(Document.objects.order_by('id').values_list('id', flat=True))))"
        )
    elif source_mode == "missing-archive":
        code = (
            "import json;"
            "from django.db.models import Q;"
            "from documents.models import Document;"
            "ids=list(Document.objects.filter(Q(archive_filename__isnull=True)|Q(archive_filename=''))."
            "order_by('id').values_list('id', flat=True));"
            "print(json.dumps(ids))"
        )
    else:
        raise ValueError(f"Unknown source mode: {source_mode}")

    result = run_manage_py(
        f"shell -c {shlex.quote(code)}",
        manage_root=manage_root,
        manage_python=manage_python,
        exec_mode=exec_mode,
    )
    try:
        data = parse_json_from_mixed_output(result.stdout)
        if not isinstance(data, list):
            raise RuntimeError(f"Expected list from manage.py output, got: {type(data).__name__}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Could not parse ID list from manage.py output: {result.stdout.strip()}"
        ) from exc

    ids = [int(x) for x in data]
    return ids


def get_document_meta(doc_id: int, manage_root: str, manage_python: str, exec_mode: str) -> dict:
    code = (
        "import json;"
        "from documents.models import Document;"
        f"d=Document.objects.filter(pk={doc_id}).first();"
        "print(json.dumps({'exists': False} if d is None else {"
        "'exists': True,"
        "'id': d.id,"
        "'title': d.title,"
        "'mime_type': d.mime_type,"
        "'original_filename': d.original_filename,"
        "'archive_filename': d.archive_filename,"
        "'content_length': len(d.content or ''),"
        "'page_count': d.page_count,"
        "'modified': d.modified.isoformat() if d.modified else None"
        "}))"
    )
    result = run_manage_py(
        f"shell -c {shlex.quote(code)}",
        manage_root=manage_root,
        manage_python=manage_python,
        exec_mode=exec_mode,
    )
    parsed = parse_json_from_mixed_output(result.stdout)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Could not parse document metadata from manage.py output: {result.stdout.strip()}")
    return parsed


def run_archiver_for_id(
    doc_id: int,
    processes: int,
    overwrite: bool,
    manage_root: str,
    manage_python: str,
    exec_mode: str,
) -> subprocess.CompletedProcess:
    cmd = f"document_archiver --document {doc_id} --processes {processes}"
    if overwrite:
        cmd += " --overwrite"
    return run_manage_py(
        cmd,
        manage_root=manage_root,
        manage_python=manage_python,
        exec_mode=exec_mode,
        check=False,
    )


def run_preflight(manage_root: str, manage_python: str, exec_mode: str) -> tuple[bool, str]:
    code = (
        "import json;"
        "from django.db import connection;"
        "table_names=set(connection.introspection.table_names());"
        "required=['documents_document','documents_paperlesstask','django_celery_results_taskresult'];"
        "missing=[t for t in required if t not in table_names];"
        "payload={"
        "'db_vendor': connection.vendor,"
        "'required_tables': required,"
        "'missing_tables': missing"
        "};"
        "print(json.dumps(payload))"
    )
    result = run_manage_py(
        f"shell -c {shlex.quote(code)}",
        manage_root=manage_root,
        manage_python=manage_python,
        exec_mode=exec_mode,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        if "sudo:" in detail and "password" in detail.lower():
            detail += " | rerun with sudo/root OR pass --exec-mode direct"
        return False, f"manage.py preflight failed: {detail}"

    try:
        payload = parse_json_from_mixed_output(result.stdout)
        if not isinstance(payload, dict):
            return False, f"manage.py preflight produced unexpected output type: {type(payload).__name__}"
    except json.JSONDecodeError:
        return False, f"manage.py preflight produced non-JSON output: {result.stdout.strip()}"

    missing = payload.get("missing_tables") or []
    db_vendor = payload.get("db_vendor", "unknown")
    if missing:
        return (
            False,
            "DB is not ready for OCR/archive runs. "
            f"db_vendor={db_vendor}, missing_tables={','.join(missing)}",
        )

    return True, f"preflight OK (db_vendor={db_vendor})"


def derive_report_paths(log_path: Path) -> tuple[Path, Path]:
    base = log_path.with_suffix("") if log_path.suffix == ".log" else log_path
    return Path(f"{base}.success.csv"), Path(f"{base}.failed.csv")


def write_result_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in RESULT_FIELDS})


def derive_excel_path(log_path: Path, excel_file: str | None) -> Path:
    if excel_file:
        return Path(excel_file)
    base = log_path.with_suffix("") if log_path.suffix == ".log" else log_path
    return Path(f"{base}.analysis.xlsx")


def excel_column_name(idx: int) -> str:
    chars: list[str] = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        chars.append(chr(65 + rem))
    return "".join(reversed(chars))


def xml_safe_text(value: object) -> str:
    text = "" if value is None else str(value)
    filtered = "".join(ch for ch in text if ch in ("\t", "\n", "\r") or ord(ch) >= 32)
    return html.escape(filtered, quote=False)


def build_sheet_xml(rows: list[dict]) -> str:
    all_rows: list[list[object]] = [RESULT_FIELDS]
    for row in rows:
        all_rows.append([row.get(name, "") for name in RESULT_FIELDS])

    row_xml_parts: list[str] = []
    for row_idx, values in enumerate(all_rows, start=1):
        cell_parts: list[str] = []
        for col_idx, value in enumerate(values, start=1):
            ref = f"{excel_column_name(col_idx)}{row_idx}"
            cell_parts.append(
                f'<c r="{ref}" t="inlineStr"><is><t>{xml_safe_text(value)}</t></is></c>'
            )
        row_xml_parts.append(f'<row r="{row_idx}">{"".join(cell_parts)}</row>')

    max_col = excel_column_name(len(RESULT_FIELDS))
    max_row = max(1, len(all_rows))
    dimension = f"A1:{max_col}{max_row}"
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="{dimension}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        "<sheetFormatPr defaultRowHeight=\"15\"/>"
        f'<sheetData>{"".join(row_xml_parts)}</sheetData>'
        "</worksheet>"
    )


def write_result_excel(path: Path, success_rows: list[dict], failed_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
"""
    rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="success" sheetId="1" r:id="rId1"/>
    <sheet name="failed" sheetId="2" r:id="rId2"/>
  </sheets>
</workbook>
"""
    workbook_rels = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>
"""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("xl/workbook.xml", workbook)
        zf.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        zf.writestr("xl/worksheets/sheet1.xml", build_sheet_xml(success_rows))
        zf.writestr("xl/worksheets/sheet2.xml", build_sheet_xml(failed_rows))


def persist_reports(
    success_path: Path,
    failed_path: Path,
    excel_path: Path,
    success_rows: list[dict],
    failed_rows: list[dict],
) -> None:
    write_result_csv(success_path, success_rows)
    write_result_csv(failed_path, failed_rows)
    write_result_excel(excel_path, success_rows, failed_rows)


def get_sudo_owner() -> tuple[int, int] | None:
    uid = os.environ.get("SUDO_UID")
    gid = os.environ.get("SUDO_GID")
    if not uid or not gid:
        return None
    try:
        return int(uid), int(gid)
    except ValueError:
        return None


def ensure_owner_rwx(path: Path, owner: tuple[int, int] | None) -> None:
    if not path.exists():
        return
    st = path.stat()
    os.chmod(path, st.st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    if owner and os.geteuid() == 0:
        os.chown(path, owner[0], owner[1])


def analyze_progress(before_meta: dict, after_meta: dict) -> dict:
    before_content_len = int(before_meta.get("content_length") or 0)
    after_content_len = int(after_meta.get("content_length") or 0)
    content_delta = after_content_len - before_content_len

    before_archive = before_meta.get("archive_filename") or ""
    after_archive = after_meta.get("archive_filename") or ""
    archive_changed = before_archive != after_archive

    before_modified = before_meta.get("modified") or ""
    after_modified = after_meta.get("modified") or ""
    modified_changed = bool(before_modified and after_modified and before_modified != after_modified)

    partial_progress = content_delta > 0 or archive_changed or modified_changed
    return {
        "partial_progress": partial_progress,
        "pre_content_length": before_content_len,
        "post_content_length": after_content_len,
        "content_delta": content_delta,
        "pre_page_count": before_meta.get("page_count", ""),
        "post_page_count": after_meta.get("page_count", ""),
        "archive_changed": archive_changed,
        "modified_changed": modified_changed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Paperless document_archiver for specific document IDs with detailed per-ID logging."
        )
    )

    source = parser.add_mutually_exclusive_group()
    source.add_argument("--id", type=int, help="Single document ID, e.g. 1821")
    source.add_argument("--ids", help="Comma-separated document IDs, e.g. 1,2,3")
    source.add_argument("--all", action="store_true", help="Process all document IDs")
    source.add_argument(
        "--missing-archive",
        action="store_true",
        help="Process IDs where archive_filename is NULL/empty (default)",
    )

    parser.add_argument("--processes", type=int, default=3, help="Processes per document run")
    parser.add_argument("--no-overwrite", action="store_true", help="Do not pass --overwrite")
    parser.add_argument("--dry-run", action="store_true", help="Show selected IDs and metadata only")
    parser.add_argument(
        "--manage-root",
        default=MANAGE_ROOT,
        help=f"Path containing manage.py (default: {MANAGE_ROOT})",
    )
    parser.add_argument(
        "--manage-python",
        default=MANAGE_PYTHON,
        help=f"Python executable for manage.py (default: {MANAGE_PYTHON})",
    )
    parser.add_argument(
        "--exec-mode",
        default=EXEC_MODE_SUDO,
        choices=VALID_EXEC_MODES,
        help="Execution mode: sudo (default), auto, or direct",
    )
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Only run environment/DB checks and exit",
    )
    parser.add_argument(
        "--log-file",
        default=str(
            DEFAULT_OUTPUT_DIR
            / f"paperless-ocr-by-id-{dt.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"
        ),
        help="Path to detailed log output",
    )
    parser.add_argument(
        "--success-list-file",
        default=None,
        help="CSV path for OCR success rows (default: derived from --log-file)",
    )
    parser.add_argument(
        "--failed-list-file",
        default=None,
        help="CSV path for OCR failure rows (default: derived from --log-file)",
    )
    parser.add_argument(
        "--excel-file",
        default=None,
        help="Excel .xlsx path for analysis output (default: derived from --log-file)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=0,
        help="Randomly sample this many IDs from the selected pool (0 = no sampling)",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible sampling",
    )

    args = parser.parse_args()

    if args.processes < 1:
        print("--processes must be >= 1", file=sys.stderr)
        return 2
    if args.sample_size < 0:
        print("--sample-size must be >= 0", file=sys.stderr)
        return 2

    ids_csv_value = str(args.id) if args.id is not None else args.ids
    source_mode = "ids" if ids_csv_value else ("all" if args.all else "missing-archive")
    overwrite = not args.no_overwrite

    ok, preflight_message = run_preflight(
        manage_root=args.manage_root,
        manage_python=args.manage_python,
        exec_mode=args.exec_mode,
    )
    if not ok:
        print(f"Preflight failed: {preflight_message}", file=sys.stderr)
        if "password" not in preflight_message.lower():
            print(
                "Hint: This usually means manage.py is pointed at an uninitialized DB "
                "(for example local sqlite instead of the live Paperless DB).",
                file=sys.stderr,
            )
        return 1
    print(f"Preflight: {preflight_message}")
    if args.preflight_only:
        return 0

    try:
        ids = get_ids(
            source_mode,
            ids_csv_value,
            manage_root=args.manage_root,
            manage_python=args.manage_python,
            exec_mode=args.exec_mode,
        )
    except Exception as exc:
        print(f"Failed to load IDs: {exc}", file=sys.stderr)
        return 1

    if not ids:
        print(f"No IDs found for source mode: {source_mode}")
        return 0

    original_count = len(ids)
    if args.sample_size > 0:
        if args.sample_seed is not None:
            random.seed(args.sample_seed)
        sample_n = min(args.sample_size, len(ids))
        ids = random.sample(ids, sample_n)
        print(
            f"Sampling enabled: selected {sample_n} random ID(s) "
            f"from {original_count} total (seed={args.sample_seed})"
        )
    target_count = args.sample_size if args.sample_size > 0 else len(ids)

    log_path = Path(args.log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    default_success_path, default_failed_path = derive_report_paths(log_path)
    success_list_path = Path(args.success_list_file) if args.success_list_file else default_success_path
    failed_list_path = Path(args.failed_list_file) if args.failed_list_file else default_failed_path
    excel_path = derive_excel_path(log_path, args.excel_file)
    sudo_owner = get_sudo_owner()

    with log_path.open("a", encoding="utf-8") as lf:
        lf.write(f"\n===== RUN START {dt.datetime.now().isoformat()} =====\n")
        lf.write(
            f"SOURCE_MODE={source_mode} PROCESSES={args.processes} OVERWRITE={overwrite} DRY_RUN={args.dry_run}\n"
        )
    ensure_owner_rwx(log_path, sudo_owner)

    print(f"Selected {len(ids)} document ID(s) from '{source_mode}'")
    print(f"Log file: {log_path}")
    print(f"Success list: {success_list_path}")
    print(f"Failure list: {failed_list_path}")
    print(f"Excel analysis: {excel_path}")
    print(f"Target documents to process: {target_count}")

    failed_ids: list[int] = []
    success_count = 0
    fail_partial_count = 0
    fail_no_change_count = 0
    success_rows: list[dict] = []
    failed_rows: list[dict] = []
    persist_reports(success_list_path, failed_list_path, excel_path, success_rows, failed_rows)
    ensure_owner_rwx(success_list_path, sudo_owner)
    ensure_owner_rwx(failed_list_path, sudo_owner)
    ensure_owner_rwx(excel_path, sudo_owner)

    for idx, doc_id in enumerate(ids, start=1):
        if idx > target_count:
            print(f"[STOP] Reached target_count={target_count}, stopping.")
            break
        print(f"[START] ID={doc_id}")

        meta_before: dict = {
            "id": doc_id,
            "title": "",
            "mime_type": "",
            "original_filename": "",
            "archive_filename": "",
            "content_length": 0,
            "page_count": "",
            "modified": "",
            "exists": False,
        }
        try:
            meta_before = get_document_meta(
                doc_id,
                manage_root=args.manage_root,
                manage_python=args.manage_python,
                exec_mode=args.exec_mode,
            )
            if not meta_before.get("exists"):
                print(f"[META]  ID={doc_id} | MISSING")
            else:
                print(
                    "[META]  "
                    f"ID={meta_before.get('id')} | "
                    f"TITLE={meta_before.get('title')!r} | "
                    f"MIME={meta_before.get('mime_type')} | "
                    f"ORIG={meta_before.get('original_filename')!r} | "
                    f"ARCH={meta_before.get('archive_filename')!r} | "
                    f"CONTENT_LEN={meta_before.get('content_length')}"
                )
        except Exception as exc:
            print(f"[META]  ID={doc_id} | ERROR reading metadata: {exc}")

        if args.dry_run:
            print(f"[SKIP] ID={doc_id} (dry-run)")
            continue

        result = run_archiver_for_id(
            doc_id,
            args.processes,
            overwrite,
            manage_root=args.manage_root,
            manage_python=args.manage_python,
            exec_mode=args.exec_mode,
        )

        with log_path.open("a", encoding="utf-8") as lf:
            lf.write(f"[{doc_id}] CMD: document_archiver --document {doc_id} --processes {args.processes}")
            if overwrite:
                lf.write(" --overwrite")
            lf.write("\n")
            if result.stdout:
                lf.write(result.stdout)
            if result.stderr:
                lf.write(result.stderr)
            lf.write("\n")

        meta_after = dict(meta_before)
        try:
            meta_after = get_document_meta(
                doc_id,
                manage_root=args.manage_root,
                manage_python=args.manage_python,
                exec_mode=args.exec_mode,
            )
        except Exception as exc:
            print(f"[META2] ID={doc_id} | ERROR reading metadata after run: {exc}")

        progress = analyze_progress(meta_before, meta_after)
        print(
            "[META2] "
            f"ID={doc_id} | "
            f"CONTENT_LEN_BEFORE={progress['pre_content_length']} | "
            f"CONTENT_LEN_AFTER={progress['post_content_length']} | "
            f"CONTENT_DELTA={progress['content_delta']} | "
            f"ARCHIVE_CHANGED={progress['archive_changed']}"
        )

        if result.returncode == 0:
            success_count += 1
            success_rows.append(
                {
                    "id": doc_id,
                    "title": meta_after.get("title", ""),
                    "mime_type": meta_after.get("mime_type", ""),
                    "original_filename": meta_after.get("original_filename", ""),
                    "archive_filename": meta_after.get("archive_filename", ""),
                    "status": "OK",
                    "exit_code": result.returncode,
                    **progress,
                    "error": "",
                }
            )
            print(f"[OK]    ID={doc_id}")
        else:
            failed_ids.append(doc_id)
            status = "FAIL_PARTIAL_OUTPUT" if progress["partial_progress"] else "FAIL_NO_CHANGE"
            if progress["partial_progress"]:
                fail_partial_count += 1
            else:
                fail_no_change_count += 1
            stderr_line = (result.stderr or "").strip().splitlines()
            failed_rows.append(
                {
                    "id": doc_id,
                    "title": meta_after.get("title", ""),
                    "mime_type": meta_after.get("mime_type", ""),
                    "original_filename": meta_after.get("original_filename", ""),
                    "archive_filename": meta_after.get("archive_filename", ""),
                    "status": status,
                    "exit_code": result.returncode,
                    **progress,
                    "error": stderr_line[-1] if stderr_line else "",
                }
            )
            print(
                f"[FAIL]  ID={doc_id} (exit={result.returncode}, "
                f"partial_progress={progress['partial_progress']}, "
                f"content_delta={progress['content_delta']})"
            )

        persist_reports(success_list_path, failed_list_path, excel_path, success_rows, failed_rows)
        ensure_owner_rwx(success_list_path, sudo_owner)
        ensure_owner_rwx(failed_list_path, sudo_owner)
        ensure_owner_rwx(excel_path, sudo_owner)

    persist_reports(success_list_path, failed_list_path, excel_path, success_rows, failed_rows)
    ensure_owner_rwx(success_list_path, sudo_owner)
    ensure_owner_rwx(failed_list_path, sudo_owner)
    ensure_owner_rwx(excel_path, sudo_owner)

    print()
    print(f"Summary: success={success_count} failed={len(failed_ids)} total={len(ids)}")
    if failed_ids:
        print(
            "Failure detail: "
            f"partial_output={fail_partial_count} no_change={fail_no_change_count}"
        )
    if failed_ids:
        print("Failed IDs: " + " ".join(str(x) for x in failed_ids))

    with log_path.open("a", encoding="utf-8") as lf:
        lf.write(f"===== RUN END {dt.datetime.now().isoformat()} =====\n")
        lf.write(f"Summary: success={success_count} failed={len(failed_ids)} total={len(ids)}\n")
        if failed_ids:
            lf.write(
                "Failure detail: "
                f"partial_output={fail_partial_count} no_change={fail_no_change_count}\n"
            )
        if failed_ids:
            lf.write("Failed IDs: " + " ".join(str(x) for x in failed_ids) + "\n")
    ensure_owner_rwx(log_path, sudo_owner)

    print(f"Detailed output written to: {log_path}")
    print(f"OCR success CSV written to: {success_list_path}")
    print(f"OCR failure CSV written to: {failed_list_path}")
    print(f"OCR analysis Excel written to: {excel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
