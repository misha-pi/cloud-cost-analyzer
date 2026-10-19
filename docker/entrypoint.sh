#!/bin/sh
set -eu

cd /app

ensure_env() {
  if [ ! -f viz_rill/.env ]; then
    cp viz_rill/.env.example viz_rill/.env
  fi
  # Models branch on this key. A missing key fails template rendering.
  if [ -w viz_rill/.env ] && ! grep -q '^RILL_CONNECTOR=' viz_rill/.env; then
    printf '\nRILL_CONNECTOR=""\n' >> viz_rill/.env
  fi
  mkdir -p viz_rill/data viz_rill/tmp
}

start_rill() {
  ensure_env
  echo "Rill dashboards: http://localhost:${RILL_PORT}/"
  exec rill start viz_rill \
    --no-open \
    --pull-env=false \
    --port "${RILL_PORT}"
}

prepare_demo() {
  echo "Loading bundled demo data into viz_rill/data..."
  rm -rf viz_rill/data
  mkdir -p viz_rill/data
  cp -a viz_rill/data_demo/. viz_rill/data/
  # MongoDB Atlas demo data is generated, not bundled.
  sh scripts/atlas_demo_data.sh
}

if [ "$#" -eq 0 ]; then
  set -- demo
fi

case "$1" in
  demo)
    prepare_demo
    start_rill
    ;;
  serve)
    start_rill
    ;;
  *)
    ensure_env
    exec "$@"
    ;;
esac
