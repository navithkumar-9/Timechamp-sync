from datetime import datetime
from app.core.logger import logger
from app.db.clickhouse import ch_manager
from app.tasks.celery_app import celery_app


@celery_app.task(name="app.tasks.periodic_tasks.heartbeat_health_check")
def heartbeat_health_check():
    """Periodic task running every 60 seconds to monitor worker heartbeat."""
    logger.info(f"[Celery Beat Heartbeat] Worker is active and executing tasks at {datetime.utcnow().isoformat()}")
    return {"heartbeat": True, "timestamp": datetime.utcnow().isoformat()}


@celery_app.task(name="app.tasks.periodic_tasks.optimize_clickhouse_tables")
def optimize_clickhouse_tables():
    """Nightly maintenance task triggering ClickHouse partition merges and cleanup."""
    logger.info("[Celery Beat] Triggering ClickHouse OPTIMIZE TABLE...")
    try:
        client = ch_manager.connect()
        client.command("OPTIMIZE TABLE TimeChamp FINAL")
        try:
            client.command("OPTIMIZE TABLE timechamp_tracking_summary FINAL")
        except Exception:
            pass
        logger.info("[Celery Beat] ClickHouse TimeChamp table optimization completed successfully.")
        return {"status": "SUCCESS"}
    except Exception as e:
        logger.warning(f"[Celery Beat] Notice during table optimization: {e}")
        return {"status": "FAILED", "reason": str(e)}


@celery_app.task(name="app.tasks.periodic_tasks.periodic_timechamp_sync")
def periodic_timechamp_sync():
    """Scheduled task pulling tracking summary data from Timechamp API into ClickHouse."""
    import asyncio
    from app.integrations.timechamp.pipeline import get_sync_pipeline

    logger.info("[Celery Beat Scheduled] Executing periodic Timechamp data sync...")
    pipeline = get_sync_pipeline()
    result = asyncio.run(pipeline.run_sync(sync_type="SCHEDULED_CRON", auto_catchup=True))
    return result.model_dump()

