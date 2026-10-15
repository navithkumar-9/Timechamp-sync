from celery import Celery
from celery.schedules import crontab
from app.core.config import settings

celery_app = Celery(
    "timechamp_worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.sample_tasks",
        "app.tasks.periodic_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_routes={
        "app.tasks.sample_tasks.generate_tenant_analytics_report": {"queue": "reports"},
        "app.tasks.sample_tasks.*": {"queue": "default"},
        "app.tasks.periodic_tasks.*": {"queue": "periodic"},
    },
    beat_schedule={
        "heartbeat-health-every-minute": {
            "task": "app.tasks.periodic_tasks.heartbeat_health_check",
            "schedule": 60.0,
        },
        "sync-timechamp-data-periodically": {
            "task": "app.tasks.periodic_tasks.periodic_timechamp_sync",
            "schedule": float(settings.TIMECHAMP_SYNC_INTERVAL_MINUTES * 60), # Every 15 minutes
        },
        "optimize-clickhouse-partitions-nightly": {
            "task": "app.tasks.periodic_tasks.optimize_clickhouse_tables",
            "schedule": crontab(hour=2, minute=0), # Every night at 02:00 UTC
        },
    },
)

