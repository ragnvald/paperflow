#!/usr/bin/env bash
set -euo pipefail

SRC_ROOT="/home/ragnvald/code/paperless-ngx/media/documents"
DST_ROOT="/opt/paperless/media/documents"
MODE="copy"

sum_kb() {
  du -sk "$@" | awk '{s += $1} END {print s+0}'
}

format_kb() {
  awk -v kb="$1" 'BEGIN {
    b=kb*1024;
    split("B KB MB GB TB", u, " ");
    i=1;
    while (b >= 1024 && i < 5) { b/=1024; i++ }
    printf "%.2f %s", b, u[i]
  }'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: sync_media_to_live_paperless.sh [options]

Options:
  --mode MODE   copy|move (default: copy)
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ "$MODE" != "copy" && "$MODE" != "move" ]]; then
  echo "Invalid --mode value: $MODE (expected: copy|move)" >&2
  exit 1
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo ./paperless/sync_media_to_live_paperless.sh"
  exit 1
fi

if [[ ! -d "$SRC_ROOT" ]]; then
  echo "Source path not found: $SRC_ROOT" >&2
  exit 1
fi

mkdir -p "$DST_ROOT"

for sub in archive originals thumbnails; do
  if [[ ! -d "$SRC_ROOT/$sub" ]]; then
    echo "Missing source subfolder: $SRC_ROOT/$sub" >&2
    exit 1
  fi
done

SOURCE_KB="$(sum_kb "$SRC_ROOT/archive" "$SRC_ROOT/originals" "$SRC_ROOT/thumbnails")"
FREE_KB="$(df -Pk "$DST_ROOT" | awk 'NR==2{print $4}')"
SRC_FS="$(df -P "$SRC_ROOT" | awk 'NR==2{print $1}')"
DST_FS="$(df -P "$DST_ROOT" | awk 'NR==2{print $1}')"
SAME_FS=0
if [[ "$SRC_FS" == "$DST_FS" ]]; then
  SAME_FS=1
fi

REQUIRED_KB=0
if [[ "$MODE" == "copy" ]]; then
  REQUIRED_KB=$((SOURCE_KB + 524288))
else
  if [[ "$SAME_FS" -eq 1 ]]; then
    REQUIRED_KB=262144
  else
    REQUIRED_KB=$((SOURCE_KB + 524288))
  fi
fi

echo "Mode:        $MODE"
echo "Same FS:     $SAME_FS ($SRC_FS -> $DST_FS)"
echo "Source size: $(format_kb "$SOURCE_KB")"
echo "Free space:  $(format_kb "$FREE_KB")"
echo "Need space:  $(format_kb "$REQUIRED_KB")"

if (( FREE_KB < REQUIRED_KB )); then
  echo "Insufficient free space for mode '$MODE'." >&2
  echo "Use --mode move on same filesystem, or free space first." >&2
  exit 1
fi

echo "Syncing media folders to live Paperless path..."
for sub in archive originals thumbnails; do
  if [[ "$MODE" == "copy" ]]; then
    mkdir -p "$DST_ROOT/$sub"
    rsync -a --delete --human-readable --info=progress2 "$SRC_ROOT/$sub/" "$DST_ROOT/$sub/"
  else
    rm -rf "$DST_ROOT/$sub"
    if [[ "$SAME_FS" -eq 1 ]]; then
      mv "$SRC_ROOT/$sub" "$DST_ROOT/$sub"
    else
      mkdir -p "$DST_ROOT/$sub"
      rsync -a --remove-source-files --human-readable --info=progress2 "$SRC_ROOT/$sub/" "$DST_ROOT/$sub/"
      find "$SRC_ROOT/$sub" -type d -empty -delete
    fi
  fi
done

echo "Fixing ownership and permissions..."
chown -R paperless:paperless /opt/paperless/media

echo "Live media counts:"
for sub in archive originals thumbnails; do
  printf '%s=' "$sub"
  find "$DST_ROOT/$sub" -type f | wc -l
done

echo "Done."