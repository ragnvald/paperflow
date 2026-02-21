# Learning log

## Object: paperless workspace
- **Contain**: Workspace root is `/home/ragnvald/code`.
- **Include**: Main related folders are `paperless/`, `paperless-ngx/`, `paperless-env/`, and `paperles_backup/`.

## Object: instructions.md
- **Direct**: Use `instructions.md` as the governing file for work in this workspace.
- **Require**: Keep learned facts in `learning.md` when they are relevant and reusable.
- **Structure**: Prefer object-first notes with short action/fact lines; allow common fields (`Location`, `Purpose`, `Constraint`, `Command`) when verb/object wording is awkward.
- **Enforce**: Ask before restructuring existing system design (especially database/backend during restore).
- **Enforce**: Pre-check free space before large copy/sync and prefer move semantics when duplication is unnecessary.

## Object: learning.md
- **Store**: Session-derived facts in a reusable object/verb format.

## Object: paperless-env
- **Provide**: Python virtual environment with Python 3.12 executables in `paperless-env/bin/`.

## Object: paperless-ngx
- **Contain**: Main application sources in `paperless-ngx/src/` and frontend in `paperless-ngx/src-ui/`.

## Object: deployment context
- **Check first**: Determine runtime mode before proposing commands (`bare metal` vs `docker`).
- **Avoid**: Do not assume Docker tooling/services exist without verification.
- **Current state**: This environment is bare metal.

## Object: paperless media restore
- **Prefer**: Clear destination media subfolders before bulk copy when space is tight.
- **Use**: Restore workflow should purge `media/documents/{archive,originals,thumbnails}` and then copy from backup.

## Object: paperless runtime services
- **Run from**: systemd units pointing to `/opt/paperless` and reading `EnvironmentFiles=/opt/paperless/paperless.conf`.
- **Observe**: Host PostgreSQL unit is masked/inactive and cluster `16/main` is down on port `5432`.
- **Implication**: Paperless cannot be actively connected to PostgreSQL while the database service is down.

## Object: backup database dump
- **Use**: Valid restore source is `/home/ragnvald/code/paperles_backup/db_backup_20260216200002.sql.gz` (gzip integrity OK).
- **Ignore**: `restore_snapshot_20260218_054340/current_db.dump` is empty (0 bytes).
- **Contain**: SQL dump includes Paperless tables with row data (`COPY public.documents_document`, `COPY public.auth_user`).

## Object: paperless database backend mismatch
- **Cause**: `paperless.conf` had `PAPERLESS_DBENGINE=sqlite`, which made Paperless show `0` docs despite restored media files.
- **Fix**: Set `PAPERLESS_DBENGINE=postgresql` and define `PAPERLESS_DBHOST/PORT/NAME/USER/PASS`.
- **Restore detail**: SQL dump required restore as `postgres` (contains role-level statements), then grant privileges to `paperless` role.
- **Result**: PostgreSQL table counts after restore were `documents_document=2053` and `auth_user=17`.

## Object: media restore destination path
- **Cause**: Initial media restore copied into `/home/ragnvald/code/paperless-ngx/media/documents` while runtime service uses `/opt/paperless/media`.
- **Effect**: Missing thumbnails and embedded PDF previews despite document metadata existing in DB.
- **Fix**: Sync `archive/originals/thumbnails` into `/opt/paperless/media/documents` and ensure ownership is `paperless:paperless`.

## Object: disk-space-safe media transfer
- **Constraint**: On tight disk, never duplicate full media trees (`archive/originals/thumbnails`) across two roots.
- **Prefer**: Move-in-place strategy (`mv` directory handoff or `rsync --remove-source-files`) over plain copy.
- **Verify first**: Confirm runtime media root from live config (`/opt/paperless/paperless.conf`) before any transfer.
- **Order**: Stop Paperless services, perform move/sync, fix ownership (`paperless:paperless`), then restart services.
- **Guardrail**: If destination write fails with `No space left on device`, immediately stop copy flow and free duplicated source data before retry.
- **Recovery used**: Deleted duplicate tree `/home/ragnvald/code/paperles_backup/documents` after confirming same size as `/home/ragnvald/code/paperless-ngx/media/documents` to reclaim ~71G.

## Object: large file operations (general)
- **Require**: Before any large copy/sync, estimate transfer size and compare against free destination space.
- **Rule**: If free space is below source size + safety margin, do not start copy.
- **Alternative**: Prefer `move` semantics over `copy` when preserving a second full copy is not required.
- **Safety margin**: Keep at least a small reserve (for example a few hundred MB) to avoid service instability during transfers.

## Object: OCR debugging script
- **Provide**: Script `/home/ragnvald/code/paperless/run_archiver_by_ids.sh` runs `document_archiver` per document ID with explicit START/OK/FAIL logging.
- **Select IDs**: Supports `--id`, `--ids`, `--all`, and `--missing-archive` (DB-driven selection).
- **Log**: Default output location is `/home/ragnvald/code/paperless/data_out/` for log, CSV, and Excel files.
- **Track DB**: Default OCR tracking SQLite path is `/home/ragnvald/code/paperless/data_memory/paperless_ocr_tracking.sqlite3`.
- **Preflight**: `/home/ragnvald/code/paperless/run_archiver_by_ids.py` now validates required DB tables before OCR runs and supports `--preflight-only`.
- **Fail mode**: If run as a non-root user without passwordless sudo to `paperless`, the script exits early with an explicit sudo/root requirement.
- **Runtime options**: Script now supports `--exec-mode {auto,sudo,direct}` and configurable `--manage-root` / `--manage-python` for local-dev vs live `/opt/paperless` execution.
- **Reports**: Each run now writes CSV lists for OCR outcomes (`*.success.csv` and `*.failed.csv`) with ID and filename metadata.
- **Partial OCR detection**: Failed rows are classified as `FAIL_PARTIAL_OUTPUT` vs `FAIL_NO_CHANGE` using before/after checks (`content_length`, `archive_filename`, `modified`).
- **Excel output**: Each run now also writes an Excel workbook (`*.analysis.xlsx`) with separate `success` and `failed` sheets containing the same analysis fields.
- **Random sampling**: Script supports `--sample-size N` and optional `--sample-seed` to process a random subset (for statistical spot checks).
- **Run robustness**: OCR reports (`success/failed/excel`) are now persisted during the run (not only at the end), and sampled runs enforce an explicit target cap.
- **Content metrics**: Terminal output now includes before/after content length and delta (`CONTENT_LEN_BEFORE`, `CONTENT_LEN_AFTER`, `CONTENT_DELTA`) to verify OCR gain.
- **Interpretation**: `archive_changed` tracks filename/path changes, not text changes; `modified_changed` may stay false when updates are done via `QuerySet.update(...)`.
- **Ownership**: When run via sudo, the script attempts to set report file ownership to the invoking user (`SUDO_UID`/`SUDO_GID`) and ensures owner `rwx`.

## Object: paperflow Python environment
- **Use**: Local virtual environment path is `/home/ragnvald/code/paperflow/.venv`.
- **Install**: Python dependencies are pinned in `/home/ragnvald/code/paperflow/requirements.txt`.
- **Depend**: Current third-party module set is `ttkbootstrap` with `pillow` (GUI script also requires system `tkinter` runtime).

## Object: paperflow API token file
- **Default path**: API token file defaults to `/home/ragnvald/code/paperflow/secrets/paperlesstoken.api`.
- **Fallback**: Legacy token path `/home/ragnvald/code/secrets/paperlesstoken.api` is still read when repo-local token file is absent.
- **Template**: Commit `/home/ragnvald/code/paperflow/secrets/paperlesstoken.api.template` and keep real token file gitignored.
