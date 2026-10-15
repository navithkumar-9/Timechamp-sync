#!/usr/bin/env bash
set -e

echo "=== [Timechamp Engine] Launching Celery Worker ==="
exec celery -A app.tasks.celery_app.celery_app worker \
    --loglevel=${CELERY_LOG_LEVEL:-INFO} \
    --concurrency=${CELERY_CONCURRENCY:-4} \
    -Q celery,default,periodic,reports \
    -E
