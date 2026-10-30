#!/bin/sh
# Daily load on the server: new GCP and MongoDB Atlas billing rows, then a Rill refresh (make run-etl).
# Installed in root's crontab by `make install-cron`. Output goes to logs/daily_load.log (last 5000 lines
# kept). A run is skipped while the previous one is still going.
set -u
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"
mkdir -p logs
LOG=logs/daily_load.log
{
  echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') daily load"
  flock -n /tmp/cloud-cost-daily-load.lock make run-etl
  status=$?
  echo "=== $(date -u '+%Y-%m-%d %H:%M:%S UTC') finished with exit code $status"
} >> "$LOG" 2>&1
tail -n 5000 "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
