#!/usr/bin/env bash
set -e

echo "=== [Timechamp Engine] Launching Lightweight Redis Stream Consumer Worker ==="
exec python -m app.run_consumer

