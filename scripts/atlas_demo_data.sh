#!/bin/sh
# Load made-up MongoDB Atlas billing data into viz_rill/data/atlas_billing (gitignored) for the demo.
# Generates documents in the production schema (scripts/generate_atlas_fixtures.py) from 1 January
# last year to today, then loads them with the real pipeline in fixture mode. No credentials needed.
#
# Used by `make demo`, `make atlas-demo-data` and the Docker demo (docker/entrypoint.sh).
# Run from the repository root. PYTHON defaults to `python`; the Makefile passes `uv run python`.
set -eu

PYTHON=${PYTHON:-python}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

START="$(( $(date -u +%Y) - 1 ))-01-01"
END=$(date -u +%Y-%m-%d)

echo "Generating Atlas demo data ($START to $END)..."
rm -rf viz_rill/data/atlas_billing
$PYTHON scripts/generate_atlas_fixtures.py --start "$START" --end "$END" --cap 10000 --out "$TMP"
# Keep this load's dlt state in the temp folder, away from the real pipeline's state.
ATLAS_BILLING_FIXTURES_DIR="$TMP" DLT_DATA_DIR="$TMP/.dlt" DESTINATION__FILESYSTEM__BUCKET_URL=viz_rill/data \
  $PYTHON pipelines/mongodb_atlas_billing_pipeline.py
