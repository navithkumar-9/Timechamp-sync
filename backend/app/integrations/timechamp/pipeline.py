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


def get_catchup_check_window(ref_date: Optional[date] = None) -> Tuple[date, date]:
    """
    Returns (start_date, end_date) representing the window that must have complete tracking data.
    Ensures both the active payroll cycle and the immediately preceding payroll cycle are verified.
    Cycles run from 26th of month M-1 to 25th of month M.
    """
    ref = ref_date or date.today()
    if ref.day >= 26:
        # Currently at start of new cycle. Preceding cycle began on 26th of month M-1.
        prev_m = 12 if ref.month == 1 else ref.month - 1
        prev_y = ref.year - 1 if ref.month == 1 else ref.year
        start_date = date(prev_y, prev_m, 26)
    else:
        # Currently inside cycle (month M-1 -> month M).
        # Cycle began on 26th of month M-1.
        # Preceding cycle began on 26th of month M-2.
        m1 = 12 if ref.month == 1 else ref.month - 1
        y1 = ref.year - 1 if ref.month == 1 else ref.year
        m2 = 12 if m1 == 1 else m1 - 1
        y2 = y1 - 1 if m1 == 1 else y1
        start_date = date(y2, m2, 26)
    return start_date, ref


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
        dates_to_sync: List[Optional[str]] = []

        # Auto-detect missing dates and restore any missing historical data
        if auto_catchup and not target_date and not payroll_cycle and not (start_date and end_date):
            try:
                # 1. Guarantee historical data (< 2026-09-26) is preserved in TimeChamp
                await self.repository.ensure_historical_data_restored()

                # 2. Check for missing/incomplete dates in payroll window
                window_start, _ = get_catchup_check_window(today)
                # Check up to yesterday; today will always be included
                missing_dates = await self.repository.get_missing_dates(
                    window_start, today - timedelta(days=1), min_records=50
                )
                if missing_dates:
                    logger.info(
                        f"[{sync_id}] [Auto-Catchup] Found {len(missing_dates)} missing/incomplete date(s) in "
                        f"payroll window ({window_start} to {today - timedelta(days=1)}). Adding to sync queue."
                    )
                    for d in missing_dates:
                        d_str = d.strftime("%d-%b-%y")
                        if d_str not in dates_to_sync:
                            dates_to_sync.append(d_str)
            except Exception as catchup_err:
                logger.warning(f"[{sync_id}] Notice during auto-catchup evaluation: {catchup_err}")

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
        elif dates_to_sync or auto_catchup or lookback_days > 0:
            # Ensure recent lookback window (today and yesterday) is always refreshed
            for offset in range(max(lookback_days, 1) + 1):
                d_str = (today - timedelta(days=offset)).strftime("%d-%b-%y")
                if d_str not in dates_to_sync:
                    dates_to_sync.append(d_str)
            dates_to_sync.sort(key=lambda s: parse_date_flexible(s))
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
