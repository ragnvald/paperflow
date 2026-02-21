#!/usr/bin/env bash
set -euo pipefail

SOURCE_ROOT="/home/ragnvald/code/paperles_backup/documents"
DEST_ROOT="/opt/paperless/media/documents"
ASSUME_YES=0
DRY_RUN=0
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

usage() {
	cat <<'EOF'
Usage: restore_paperless_backup.sh [options]

Options:
	--source-root PATH   Source documents root (default: /home/ragnvald/code/paperles_backup/documents)
	--dest-root PATH     Destination media/documents root (default: /opt/paperless/media/documents)
	--mode MODE          Transfer mode: copy or move (default: copy)
	--yes                Skip confirmation prompt
	--dry-run            Show what would happen without deleting/copying
	-h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--source-root)
			SOURCE_ROOT="$2"
			shift 2
			;;
		--dest-root)
			DEST_ROOT="$2"
			shift 2
			;;
		--mode)
			MODE="$2"
			shift 2
			;;
		--yes)
			ASSUME_YES=1
			shift
			;;
		--dry-run)
			DRY_RUN=1
			shift
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "Unknown option: $1" >&2
			usage >&2
			exit 1
			;;
	esac
done

if [[ "$MODE" != "copy" && "$MODE" != "move" ]]; then
	echo "Invalid --mode value: $MODE (expected: copy|move)" >&2
	exit 1
fi

for dir in "$SOURCE_ROOT" "$DEST_ROOT"; do
	if [[ ! -d "$dir" ]]; then
		echo "Required directory not found: $dir" >&2
		exit 1
	fi
done

for sub in archive originals thumbnails; do
	if [[ ! -d "$SOURCE_ROOT/$sub" ]]; then
		echo "Missing source subfolder: $SOURCE_ROOT/$sub" >&2
		exit 1
	fi
done

if ! command -v rsync >/dev/null 2>&1; then
	echo "rsync is required but not installed." >&2
	exit 1
fi

if [[ ! -w "$DEST_ROOT" ]]; then
	echo "Destination is not writable: $DEST_ROOT" >&2
	echo "Run with sudo or choose a writable destination." >&2
	exit 1
fi

SOURCE_KB="$(sum_kb "$SOURCE_ROOT/archive" "$SOURCE_ROOT/originals" "$SOURCE_ROOT/thumbnails")"
FREE_KB="$(df -Pk "$DEST_ROOT" | awk 'NR==2{print $4}')"
SRC_FS="$(df -P "$SOURCE_ROOT" | awk 'NR==2{print $1}')"
DST_FS="$(df -P "$DEST_ROOT" | awk 'NR==2{print $1}')"
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

echo "Source:      $SOURCE_ROOT"
echo "Destination: $DEST_ROOT"
echo "Mode:        $MODE"
echo "Same FS:     $SAME_FS ($SRC_FS -> $DST_FS)"
echo "Source size: $(format_kb "$SOURCE_KB")"
echo "Free space:  $(format_kb "$FREE_KB")"
echo "Need space:  $(format_kb "$REQUIRED_KB")"

if (( FREE_KB < REQUIRED_KB )); then
	echo "Insufficient free space for mode '$MODE'." >&2
	echo "Use --mode move on same filesystem, or free space before retrying." >&2
	exit 1
fi

if [[ "$MODE" == "copy" ]]; then
	echo "Action: purge destination subfolders, then copy from source"
else
	echo "Action: purge destination subfolders, then move from source"
fi

if [[ "$ASSUME_YES" -ne 1 ]]; then
	read -r -p "Continue? [y/N]: " reply
	if [[ ! "$reply" =~ ^[Yy]$ ]]; then
		echo "Aborted."
		exit 0
	fi
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
	echo "DRY RUN: would remove destination subfolders and transfer with mode '$MODE'"
	for sub in archive originals thumbnails; do
		echo "Would remove: $DEST_ROOT/$sub"
		echo "Would $MODE:   $SOURCE_ROOT/$sub/ -> $DEST_ROOT/$sub/"
	done
	exit 0
fi

echo "Purging destination subfolders..."
rm -rf "$DEST_ROOT/archive" "$DEST_ROOT/originals" "$DEST_ROOT/thumbnails"
mkdir -p "$DEST_ROOT/archive" "$DEST_ROOT/originals" "$DEST_ROOT/thumbnails"

if [[ "$MODE" == "copy" ]]; then
	echo "Copying files..."
	for sub in archive originals thumbnails; do
		echo "Copying $sub"
		rsync -a --human-readable --info=progress2 "$SOURCE_ROOT/$sub/" "$DEST_ROOT/$sub/"
	done
else
	echo "Moving files..."
	for sub in archive originals thumbnails; do
		echo "Moving $sub"
		rm -rf "$DEST_ROOT/$sub"
		if [[ "$SAME_FS" -eq 1 ]]; then
			mv "$SOURCE_ROOT/$sub" "$DEST_ROOT/$sub"
		else
			mkdir -p "$DEST_ROOT/$sub"
			rsync -a --remove-source-files --human-readable --info=progress2 "$SOURCE_ROOT/$sub/" "$DEST_ROOT/$sub/"
			find "$SOURCE_ROOT/$sub" -type d -empty -delete
		fi
	done
fi

echo "Restore transfer complete."
