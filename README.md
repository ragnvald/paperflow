# paperflow
`paperflow` is a practical OCR operations layer on top of Paperless-ngx.

The project helps you inspect document OCR quality, rerun OCR with different engines, track what happened per document, and export clean text files for RAG ingestion.

## Project purpose
Paperless stores many documents, but OCR quality is not always good enough for downstream AI search and retrieval.

This repo gives you a user-facing pipeline to:
1. Pull document metadata/content from Paperless via API.
2. Identify weak OCR candidates.
3. Re-run OCR using either Paperless internal OCR or an OpenAI-compatible LLM OCR flow.
4. Track all OCR and export events in local SQLite.
5. Export structured `.md` / `.json` files for RAG ingestion folders.

## What you can achieve with this code
- Build and maintain a local OCR tracking database (`data_memory`) for audit/history.
- Run targeted OCR on selected documents instead of reprocessing everything.
- Compare OCR outcomes by engine (`paperless_internal` vs `llm_openai_compatible`).
- Keep a clear per-document event trail: run status, notes, exported paths, hashes.
- Export normalized text bundles to `data_out/rag_ingestion/...` for ingestion pipelines.
- Optionally write LLM OCR text back to Paperless document content.

## Main components
- `ocr_tracking_dashboard.py`: desktop control center for OCR + export workflow, including candidate selection, PDF filters, run history, logs, and progress.
- `init_ocr_tracking_db.py`: API-driven sync tool that snapshots Paperless document state into SQLite; safe to run repeatedly.
- `run_archiver_by_ids.py`: optional direct Paperless host script using `manage.py document_archiver` for local/server-side operations.

## Data model and folders
- `data_memory/`: persistent working state, including `paperless_ocr_tracking.sqlite3`, `ocr_pipeline.sqlite3`, and logs/history.
- `data_out/`: export artifacts for downstream systems. RAG exports are written under `data_out/rag_ingestion/<engine>/<doc_id>/...`.
- `secrets/`: token files (real secret files are gitignored; templates are committed).

## API-first workflow
1. Sync document state from Paperless to local SQLite with `init_ocr_tracking_db.py`.
2. Open `ocr_tracking_dashboard.py`.
3. Refresh data and build candidate sets (low text, prospective reruns, PDF filters).
4. Run OCR on selected documents with either `paperless_internal` (Paperless reprocess) or `llm_openai_compatible` (PDF to LLM API).
5. Review status and logs in the dashboard.
6. Export selected documents to RAG output as `md`, `json`, or both.
7. Re-check pipeline history and output files.

## What this is not
- Not a vector database.
- Not an embedding pipeline.
- Not a replacement for Paperless itself.

This repo prepares and curates OCR text so your own RAG stack can ingest reliable content.

## Setup and requirements
### Python setup
1. Create a virtual environment: `python3 -m venv .venv`
2. Activate it: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`

If you do not activate the venv, run scripts with `.venv/bin/python`.

### Requirements
- Python `3.12+`
- Tk runtime for GUI scripts (`tkinter`; on Debian/Ubuntu install `python3-tk` if missing)
- Python packages listed in `requirements.txt`

## Paperless API token
- Template (committed): `secrets/paperlesstoken.api.template`
- Real token file (gitignored): `secrets/paperlesstoken.api`

Setup:
1. `cp secrets/paperlesstoken.api.template secrets/paperlesstoken.api`
2. Replace the placeholder with your Paperless API token only (single line).

## LLM API key
- Recommended key file (gitignored): `secrets/openai.api`
- Legacy fallback file (also supported): `secrets/openai.token`
- Additional supported source: `OPENAI_API_KEY` environment variable.
- Additional supported source: `LLM API Key` field in dashboard settings.

## LLM OCR network controls
- Dashboard setting: `LLM Timeout (s)` default `180`.
- Dashboard setting: `Retry attempts` default `2`.

These are separate from Paperless API timeout and are important for larger PDF uploads.

## Quick start
1. Sync metadata from Paperless:
```bash
.venv/bin/python init_ocr_tracking_db.py
```
2. Launch dashboard:
```bash
.venv/bin/python ocr_tracking_dashboard.py
```
3. In the dashboard, refresh data, select candidates, run OCR with desired engine, then export selected results to the RAG folder.

## Optional direct Paperless host flow
If you run this on the same host as Paperless and need `document_archiver` control:
```bash
.venv/bin/python run_archiver_by_ids.py --ids 1717,1723
```

This mode depends on Paperless `manage.py` paths/permissions and is separate from the API-first dashboard flow.
