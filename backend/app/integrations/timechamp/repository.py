from datetime import date, datetime
from typing import Any, Dict, List, Optional
from app.core.config import settings
from app.core.logger import logger
from app.db.clickhouse import ClickHouseManager, ch_manager
from app.integrations.timechamp.interfaces import ITrackingSummaryRepository
from app.integrations.timechamp.models import (
    SyncHistoryRecord,
    TrackingSummaryFilter,
    TrackingSummaryRecord,
)

TRACKING_SUMMARY_COLUMNS = [
    "id",
    "emp_id",
    "username",
    "name",
    "department_name",
    "reporting_head_name",
    "timesheet_date",
    "timesheet_date_raw",
    "start_date_time",
    "start_date_time_raw",
    "finish_date_time",
    "finish_date_time_raw",
    "total_time",
    "working_time",
    "productive_time",
    "idle_time",
    "unproductive_time",
    "neutral_time",
    "idle_time_spent_on_productive_applications",
    "idle_time_spent_on_unproductive_applications",
    "idle_time_spent_on_neutral_applications",
    "total_productive_time",
    "total_unproductive_time",
    "total_neutral_time",
    "total_activity_count",
    "productive_activity_count",
    "unproductive_activity_count",
    "neutral_activity_count",
    "productive_percentage",
    "working_percentage",
    "fetched_at",
    "updated_at",
    "total_hours",
    "working_hours",
    "productive_hours",
    "idle_hours",
    "unproductive_hours",
    "neutral_hours",
    "idle_hours_spent_on_productive_applications",
    "idle_hours_spent_on_unproductive_applications",
    "idle_hours_spent_on_neutral_applications",
    "total_productive_hours",
    "total_unproductive_hours",
    "total_neutral_hours",
]

SYNC_HISTORY_COLUMNS = [
    "sync_id",
    "sync_type",
    "status",
    "records_fetched",
    "records_inserted",
    "duration_ms",
    "error_message",
    "started_at",
    "completed_at",
]


class ClickHouseTrackingSummaryRepository(ITrackingSummaryRepository):
    """ClickHouse repository responsible for high-performance batch storage and queries."""

    def __init__(self, ch: ClickHouseManager = ch_manager):
        self.ch = ch

    async def save_batch(self, records: List[TrackingSummaryRecord]) -> int:
        if not records:
            return 0

        rows = []
        for r in records:
            rows.append([
                str(r.id),
                r.emp_id,
                r.username,
                r.name,
                r.department_name,
                r.reporting_head_name,
                r.timesheet_date,
                r.timesheet_date_raw,
                r.start_date_time,
                r.start_date_time_raw,
                r.finish_date_time,
                r.finish_date_time_raw,
                r.total_time,
                r.working_time,
                r.productive_time,
                r.idle_time,
                r.unproductive_time,
                r.neutral_time,
                r.idle_time_spent_on_productive_applications,
                r.idle_time_spent_on_unproductive_applications,
                r.idle_time_spent_on_neutral_applications,
                r.total_productive_time,
                r.total_unproductive_time,
                r.total_neutral_time,
                r.total_activity_count,
                r.productive_activity_count,
                r.unproductive_activity_count,
                r.neutral_activity_count,
                float(r.productive_percentage),
                float(r.working_percentage),
                r.fetched_at,
                r.updated_at,
                float(r.total_hours),
                float(r.working_hours),
                float(r.productive_hours),
                float(r.idle_hours),
                float(r.unproductive_hours),
                float(r.neutral_hours),
                float(r.idle_hours_spent_on_productive_applications),
                float(r.idle_hours_spent_on_unproductive_applications),
                float(r.idle_hours_spent_on_neutral_applications),
                float(r.total_productive_hours),
                float(r.total_unproductive_hours),
                float(r.total_neutral_hours),
            ])

        table_name = f"{settings.CLICKHOUSE_DB}.timechamp_tracking_summary"
        logger.info(f"Persisting batch of {len(rows)} records into {table_name}...")
        await self.ch.insert(table="timechamp_tracking_summary", data=rows, column_names=TRACKING_SUMMARY_COLUMNS)
        logger.info(f"✓ Successfully inserted {len(rows)} tracking summary rows into ClickHouse.")
        return len(rows)

    async def save_sync_history(self, log: SyncHistoryRecord) -> None:
        row = [[
            str(log.sync_id),
            log.sync_type,
            log.status,
            log.records_fetched,
            log.records_inserted,
            float(log.duration_ms),
            log.error_message,
            log.started_at,
            log.completed_at,
        ]]
        try:
            await self.ch.insert(table="timechamp_sync_history", data=row, column_names=SYNC_HISTORY_COLUMNS)
        except Exception as e:
            logger.error(f"Failed to record sync history to ClickHouse: {e}")

    async def query_tracking_summary(self, filters: TrackingSummaryFilter) -> List[Dict[str, Any]]:
        conditions = []
        if filters.emp_id:
            conditions.append(f"emp_id = '{filters.emp_id}'")
        if filters.department_name:
            conditions.append(f"department_name = '{filters.department_name}'")
        if filters.start_date:
            conditions.append(f"timesheet_date >= '{filters.start_date.isoformat()}'")
        if filters.end_date:
            conditions.append(f"timesheet_date <= '{filters.end_date.isoformat()}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                id,
                emp_id,
                username,
                name,
                department_name,
                reporting_head_name,
                timesheet_date,
                timesheet_date_raw,
                start_date_time,
                finish_date_time,
                total_time,
                total_hours,
                working_time,
                working_hours,
                productive_time,
                productive_hours,
                idle_time,
                idle_hours,
                unproductive_time,
                unproductive_hours,
                neutral_time,
                neutral_hours,
                productive_percentage,
                working_percentage,
                updated_at
            FROM {settings.CLICKHOUSE_DB}.timechamp_tracking_summary
            {where_clause}
            ORDER BY timesheet_date DESC, department_name ASC, emp_id ASC
            LIMIT {filters.limit} OFFSET {filters.offset}
        """
        return await self.ch.query(query)

    async def get_latest_synced_date(self) -> Optional[date]:
        """Fetch the most recent timesheet_date stored in ClickHouse."""
        query = f"SELECT max(timesheet_date) as max_date FROM {settings.CLICKHOUSE_DB}.timechamp_tracking_summary"
        try:
            rows = await self.ch.query(query)
            if rows and rows[0].get("max_date"):
                val = rows[0]["max_date"]
                if isinstance(val, date):
                    return val
                elif isinstance(val, str):
                    return datetime.strptime(val, "%Y-%m-%d").date()
        except Exception as e:
            logger.warning(f"Could not retrieve latest timesheet_date from ClickHouse: {e}")
        return None


default_repository = ClickHouseTrackingSummaryRepository()


def get_repository() -> ITrackingSummaryRepository:
    return default_repository
