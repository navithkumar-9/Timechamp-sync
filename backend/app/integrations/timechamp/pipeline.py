import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
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


def parse_date_flexible(d_str: str) -> date:
    """Parse arbitrary date string into Python date."""
    d_clean = d_str.strip()
    for fmt in ["%Y-%m-%d", "%d-%b-%y", "%d-%b-%Y", "%b-%d-%Y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(d_clean, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unsupported date format: {d_str}")


def get_payroll_cycle_range(ref_date: Optional[date] = None, cycle_str: Optional[str] = None) -> Tuple[date, date]:
    """
    Computes (start_date, end_date) for a payroll cycle.
    In the company's payroll policy, cycles run from the 26th of the prior month to the 25th of the cycle month.
    e.g. cycle_str='2026-10' -> 2026-09-26 to 2026-10-25.
    """
    if cycle_str:
        parts = cycle_str.strip().split("-")
        year, month = int(parts[0]), int(parts[1])
        end_date = date(year, month, 25)
        start_date = date(year - 1, 12, 26) if month == 1 else date(year, month - 1, 26)
        return start_date, end_date

    ref = ref_date or date.today()
    if ref.day >= 26:
        start_date = date(ref.year, ref.month, 26)
        end_date = date(ref.year + 1, 1, 25) if ref.month == 12 else date(ref.year, ref.month + 1, 25)
    else:
        end_date = date(ref.year, ref.month, 25)
        start_date = date(ref.year - 1, 12, 26) if ref.month == 1 else date(ref.year, ref.month - 1, 26)
    return start_date, end_date


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
        auto_catchup: bool = False,
        payroll_cycle: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> SyncResult:
        sync_id = uuid4()
        started_at = datetime.utcnow()
        t0 = time.perf_counter()

        today = date.today()

        # Auto-detect server downtime and expand lookback window to catch up missing dates
        if auto_catchup and not target_date and not payroll_cycle and not (start_date and end_date):
            try:
                latest_date = await self.repository.get_latest_synced_date()
                if latest_date:
                    gap_days = (today - latest_date).days
                    if gap_days > 1:
                        # Add 1 extra day lookback for offline hours flush, capped at 30 days
                        effective_lookback = min(max(lookback_days, gap_days + 1), 30)
                        logger.info(
                            f"[{sync_id}] [Auto-Catchup] Server downtime detected: "
                            f"last synced date in ClickHouse was {latest_date} ({gap_days} days ago). "
                            f"Automatically backfilling missing dates with lookback={effective_lookback} days."
                        )
                        lookback_days = effective_lookback
            except Exception as catchup_err:
                logger.warning(f"[{sync_id}] Notice during auto-catchup evaluation: {catchup_err}")

        dates_to_sync: List[Optional[str]] = []
        if payroll_cycle or (start_date and end_date):
            if payroll_cycle:
                c_start, c_end = get_payroll_cycle_range(cycle_str=payroll_cycle)
                logger.info(f"[{sync_id}] [Payroll Cycle] Resolved cycle '{payroll_cycle}' to: {c_start} .. {c_end}")
            else:
                c_start = parse_date_flexible(start_date)
                c_end = parse_date_flexible(end_date)
                logger.info(f"[{sync_id}] [Custom Range] Resolved date range: {c_start} .. {c_end}")
            curr = c_start
            while curr <= c_end:
                dates_to_sync.append(curr.strftime("%d-%b-%y"))
                curr += timedelta(days=1)
        elif target_date:
            dates_to_sync = [target_date]
        elif lookback_days > 0:
            for offset in range(lookback_days + 1):
                d = today - timedelta(days=offset)
                dates_to_sync.append(d.strftime("%d-%b-%y"))
        else:
            dates_to_sync = [None] # Default API today

        logger.info(f"[{sync_id}] === Starting Timechamp Tracking Sync ({sync_type}, dates={len(dates_to_sync)}) ===")
        total_fetched = 0
        total_inserted = 0

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
                    records_fetched=total_fetched,
                    records_inserted=total_inserted,
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
                records_fetched=total_fetched,
                records_inserted=total_inserted,
                duration_ms=duration_ms,
                message="Sync pipeline encountered an error",
                error=error_msg,
            )


default_pipeline = TrackingSyncPipeline()


def get_sync_pipeline() -> ITrackingPipeline:
    return default_pipeline
