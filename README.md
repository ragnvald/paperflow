# paperflow
Code that supports the flow of data from Paperless into a RAG-oriented workflow.

## Python setup
1. Create a virtual environment: `python3 -m venv .venv`
2. Activate it: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`

If you do not activate the venv, run scripts with `.venv/bin/python`.

## Requirements
- Python `3.12+`
- Tk runtime for GUI scripts (`tkinter`; on Debian/Ubuntu install `python3-tk` if missing)
- Python packages listed in `requirements.txt`

## Main scripts
- `init_ocr_tracking_db.py`: initialize/sync OCR tracking SQLite from the Paperless API.
- `run_archiver_by_ids.py`: run `document_archiver` per document id and write analysis reports.
- `ocr_tracking_dashboard.py`: desktop dashboard for OCR rerun flows.
