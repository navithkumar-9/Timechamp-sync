import time
from typing import Any, Dict
from app.core.logger import logger
from app.db.clickhouse import ch_manager
from app.tasks.celery_app import celery_app


@celery_app.task(bind=True, name="app.tasks.sample_tasks.generate_tenant_analytics_report")
def generate_tenant_analytics_report(self, tenant_id: str, report_type: str = "daily") -> Dict[str, Any]:
    """Background task simulating a CPU/IO-heavy analytical aggregation."""
    logger.info(f"[Task {self.request.id}] Generating {report_type} report for tenant {tenant_id}...")
    try:
        # In a real system, compute large aggregations or generate PDF/CSV
        client = ch_manager.connect()
        query = f"""
            SELECT metric_name, count() as total_records, avg(value) as average_val
            FROM {ch_manager._client.database}.metrics
            WHERE tenant_id = '{tenant_id}'
            GROUP BY metric_name
        """
        result = client.query(query)
        stats = [dict(zip(result.column_names, row)) for row in result.result_rows]

        logger.info(f"[Task {self.request.id}] Completed report successfully.")
        return {
            "status": "COMPLETED",
            "tenant_id": tenant_id,
            "report_type": report_type,
            "metrics_summary": stats,
            "generated_at": time.time(),
        }
    except Exception as e:
        logger.error(f"[Task {self.request.id}] Report generation error: {e}")
        return {"status": "FAILED", "error": str(e)}


@celery_app.task(bind=True, name="app.tasks.sample_tasks.purge_expired_temporary_data")
def purge_expired_temporary_data(self) -> Dict[str, Any]:
    """Maintenance task for purging old transient state."""
    logger.info(f"[Task {self.request.id}] Purging transient data...")
    return {"status": "SUCCESS", "purged_records": 0}


@celery_app.task(bind=True, name="app.tasks.sample_tasks.sync_timechamp_tracking_data_task")
def sync_timechamp_tracking_data_task(self, sync_type: str = "CELERY_ASYNC") -> Dict[str, Any]:
    """Celery background worker task executing Timechamp tracking sync."""
    import asyncio
    from app.integrations.timechamp.pipeline import get_sync_pipeline

    logger.info(f"[Task {self.request.id}] Running Timechamp data sync...")
    pipeline = get_sync_pipeline()
    result = asyncio.run(pipeline.run_sync(sync_type=sync_type, auto_catchup=True))
    return result.model_dump()

