#!/usr/bin/env sh
set -e

python -m inventory_alerts.cli init-db
# optional: seed demo (comment out after first run)
python -m inventory_alerts.cli seed-demo

exec python -m inventory_alerts.cli run-worker
