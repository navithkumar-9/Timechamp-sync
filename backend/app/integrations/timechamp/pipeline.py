import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4
from app.core.logger import logger
from app.integrations.timechamp.client import get_timechamp_api_client
from app.integrations.timechamp.interfaces import (
    ITimeChampClient,
    ITrackingDataTransformer,
    ITrackingPipeline,
    ITrackingSummaryRepository,
)
from app.integrations.timechamp.models import SyncHistoryRecord, SyncResult
from app.integrations.timechamp.repository import get_repository
from app.integrations.timechamp.transformer import get_transformer
from app.streams.producer import stream_producer



class TrackingSyncPipeline(ITrackingPipeline):
    """Production ETL Pipeline orchestrating the extraction, transformation, and ClickHouse persistence."""

    def __init__(
        self,
        client: ITimeChampClient = None,
        transformer: ITrackingDataTransformer = None,
        repository: ITrackingSummaryRepository = None,
    ):
        self.client = client or get_timechamp_api_client()
        self.transformer = transformer or get_transformer()
        self.repository = repository or get_repository()

    async def run_sync(
        self,
        sync_type: str = "MANUAL",
        target_date: Optional[str] = None,
        lookback_days: int = 1,
    ) -> SyncResult:
        sync_id = uuid4()
        started_at = datetime.utcnow()
        t0 = time.perf_counter()

        logger.info(f"[{sync_id}] === Starting Timechamp Tracking Sync ({sync_type}, lookback={lookback_days}) ===")
        total_fetched = 0
        total_inserted = 0

        # Determine target dates to synchronize
        from datetime import date, timedelta
        dates_to_sync: List[Optional[str]] = []
        if target_date:
            dates_to_sync = [target_date]
        elif lookback_days > 0:
            today = date.today()
            for offset in range(lookback_days + 1):
                d = today - timedelta(days=offset)
                dates_to_sync.append(d.strftime("%d-%b-%y"))
        else:
            dates_to_sync = [None] # Default API today

        try:
            for dt in dates_to_sync:
                dt_label = dt or "today"
                logger.info(f"[{sync_id}] Fetching tracking summary for date: {dt_label}...")
                raw_records = await self.client.get_tracking_summary_data(target_date=dt)
                records_fetched = len(raw_records)
                total_fetched += records_fetched

                if records_fetched > 0:
                    domain_records = self.transformer.transform_batch(raw_records)
                    inserted = await self.repository.save_batch(domain_records)
                    total_inserted += inserted
                    logger.info(f"[{sync_id}] Processed {inserted} records for date: {dt_label}")

            # Asynchronously notify Redis Stream of completed sync
            try:
                await stream_producer.publish_event(
                    event_type="timechamp_sync",
                    payload={
                        "sync_id": str(sync_id),
                        "sync_type": sync_type,
                        "records_inserted": total_inserted,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )
            except Exception as stream_err:
                logger.debug(f"Stream notification skipped: {stream_err}")


            duration_ms = round((time.perf_counter() - t0) * 1000, 2)

            # Record successful sync log in ClickHouse
            sync_log = SyncHistoryRecord(
                sync_id=sync_id,
                sync_type=sync_type,
                status="SUCCESS",
                records_fetched=total_fetched,
                records_inserted=total_inserted,
                duration_ms=duration_ms,
                error_message="",
                started_at=started_at,
                completed_at=datetime.utcnow(),
            )
            await self.repository.save_sync_history(sync_log)

            logger.info(f"[{sync_id}] ✓ Sync finished in {duration_ms}ms ({total_inserted} rows inserted/updated).")
            return SyncResult(
                success=True,
                sync_id=str(sync_id),
                records_fetched=total_fetched,
                records_inserted=total_inserted,
                duration_ms=duration_ms,
                message=f"Successfully synchronized {total_inserted} records across target dates",
            )


        except Exception as exc:
            duration_ms = round((time.perf_counter() - t0) * 1000, 2)
            error_msg = str(exc)
            logger.error(f"[{sync_id}] Sync pipeline failure: {error_msg}")

            # Persist failure log for observability
            try:
                failure_log = SyncHistoryRecord(
                    sync_id=sync_id,
                    sync_type=sync_type,
                    status="FAILED",
                    records_fetched=records_fetched,
                    records_inserted=records_inserted,
                    duration_ms=duration_ms,
                    error_message=error_msg,
                    started_at=started_at,
                    completed_at=datetime.utcnow(),
                )
                await self.repository.save_sync_history(failure_log)
            except Exception as log_err:
                logger.error(f"Could not persist failure log: {log_err}")

            return SyncResult(
                success=False,
                sync_id=str(sync_id),
                records_fetched=records_fetched,
                records_inserted=records_inserted,
                duration_ms=duration_ms,
                message="Sync pipeline encountered an error",
                error=error_msg,
            )


default_pipeline = TrackingSyncPipeline()


def get_sync_pipeline() -> ITrackingPipeline:
    return default_pipeline
