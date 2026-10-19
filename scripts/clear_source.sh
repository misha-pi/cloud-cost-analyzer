#!/bin/sh
# Delete one source's local data, so its next load reloads it in full. Other sources are untouched.
#
# Usage: CONFIRM=1 sh scripts/clear_source.sh DATASET [RELOAD_TARGET]
#   DATASET        folder under viz_rill/data (gcp_costs, atlas_billing, ...)
#   RELOAD_TARGET  make target that reloads it (shown afterwards), e.g. run-gcp
# Without CONFIRM=1 it only shows what would be deleted. `make clear-gcp`, `make clear-atlas` and
# `make clear-source SOURCE=...` call this.
#
# GCP (`make run-gcp`) and Atlas (`make run-atlas`) reload in full by themselves once their folder is
# gone. Other pipelines keep an incremental cursor in dlt's local state: run `make dlt-clear` as well.
set -eu

DATASET=${1:-}
RELOAD=${2:-}
case "$DATASET" in
  ""|.*|*/*)
    echo "ERROR: give a dataset folder name under viz_rill/data, e.g. gcp_costs (got '$DATASET')" >&2
    exit 1
    ;;
esac

DIR="viz_rill/data/$DATASET"
if [ ! -d "$DIR" ]; then
  echo "Nothing to clear: $DIR does not exist."
  exit 0
fi

echo "Folder:    $DIR"
echo "Size:      $(du -sh "$DIR" | cut -f1)"
echo "Parquet:   $(find "$DIR" -name '*.parquet' | wc -l | tr -d ' ') files"
LAST_LOAD=$(ls -t "$DIR"/_dlt_loads/* 2>/dev/null | head -1 || true)
if [ -n "$LAST_LOAD" ]; then
  echo "Last load: $(date -r "$LAST_LOAD" '+%Y-%m-%d %H:%M %Z')"
fi
if [ -f "$DIR/_demo_data" ]; then
  echo "Contents:  demo data"
else
  echo "Contents:  real data loaded by a pipeline"
fi

if [ "${CONFIRM:-0}" != "1" ]; then
  echo ""
  echo "Nothing deleted. Run again with CONFIRM=1 to delete this folder."
  exit 1
fi

rm -rf "$DIR"
echo ""
echo "Deleted $DIR."
if [ -n "$RELOAD" ]; then
  echo "Reload it with: make $RELOAD"
fi
echo "If 'make serve' is running, restart it after the reload: Rill does not retry models whose files were missing."
