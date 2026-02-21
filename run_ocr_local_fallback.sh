#!/usr/bin/env bash
set -euo pipefail

# Local fallback runner for OCR (manage.py/document_archiver path).
# Keeps host-side execution available if API-triggered reruns are unavailable.
#
# Example:
#   ./paperless/run_ocr_local_fallback.sh --ids 10,11,12 --processes 2
#   ./paperless/run_ocr_local_fallback.sh --missing-archive --sample-size 25 --processes 1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run_archiver_by_ids.py"
DEFAULT_EXEC_MODE="sudo"

if [[ ! -x "${RUNNER}" ]]; then
  echo "Missing executable runner: ${RUNNER}" >&2
  exit 1
fi

has_exec_mode=0
for arg in "$@"; do
  if [[ "${arg}" == "--exec-mode" ]]; then
    has_exec_mode=1
    break
  fi
done

if [[ "${has_exec_mode}" -eq 1 ]]; then
  exec "${RUNNER}" "$@"
else
  exec "${RUNNER}" --exec-mode "${DEFAULT_EXEC_MODE}" "$@"
fi
