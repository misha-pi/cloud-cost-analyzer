#!/bin/sh
# Daily load on the server: new GCP, MongoDB Atlas and AWS billing data, then a Rill refresh.
# Installed in root's crontab by `make install-cron`. Output goes to logs/daily_load.log (last 5000 lines
# kept). A run is skipped while the previous one is still going.
# Each source runs on its own, so one failing source does not stop the others or the Rill refresh; the
# run exits with an error when any source failed.
set -u
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
mkdir -p logs
LOG=logs/daily_load.log

stamp() { date -u '+%Y-%m-%d %H:%M:%S UTC'; }

exec 9> /tmp/cloud-cost-daily-load.lock
if ! flock -n 9; then
  echo "=== $(stamp) skipped: the previous daily load is still running" >> "$LOG"
  exit 0
fi

failed=""
{
  echo "=== $(stamp) daily load"
  for source in gcp atlas aws; do
    echo "--- $(stamp) $source"
    if make --no-print-directory "run-$source" SKIP_RILL_REFRESH=1; then
      echo "--- $(stamp) $source ok"
    else
      echo "--- $(stamp) $source FAILED"
      failed="$failed $source"
    fi
  done
  make --no-print-directory rill-refresh
  echo "=== $(stamp) finished${failed:+, failed:$failed}"
} >> "$LOG" 2>&1
tail -n 5000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
[ -z "$failed" ]
