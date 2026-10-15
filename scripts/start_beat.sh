#!/usr/bin/env bash
set -e

echo "=== [Timechamp Engine] Launching Celery Beat Scheduler ==="
# Remove any stale schedule file to prevent lockups on restart
rm -f /tmp/celerybeat-schedule
exec celery -A app.tasks.celery_app.celery_app beat \
    --loglevel=${CELERY_LOG_LEVEL:-INFO} \
    --schedule=/tmp/celerybeat-schedule
