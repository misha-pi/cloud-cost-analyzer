#!/bin/sh
# Refuse to overwrite real billing data with demo data.
#
# Usage: sh scripts/check_no_real_data.sh DATASET_DIR...
# Exits 1 when a DATASET_DIR holds parquet files but no `_demo_data` marker, i.e. data loaded by a
# real pipeline (run-gcp, run-atlas). Demo data is marked with `_demo_data` by `make demo` and
# scripts/atlas_demo_data.sh, and the real pipelines delete the marker when they load.
# Set FORCE=1 to overwrite anyway.
set -eu

[ "${FORCE:-0}" = "1" ] && exit 0

real=""
for dir in "$@"; do
  [ -d "$dir" ] || continue
  [ -f "$dir/_demo_data" ] && continue
  if [ -n "$(find "$dir" -name '*.parquet' -print -quit)" ]; then
    real="$real $dir"
  fi
done

if [ -n "$real" ]; then
  echo "ERROR: these folders hold real billing data, not demo data:$real" >&2
  echo "Demo data would replace them. On the server, use 'make serve' to view real data." >&2
  echo "To replace them anyway: FORCE=1 make <target> (reload real data later with 'make run-etl')." >&2
  exit 1
fi
