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

TIMECHAMP_LEGACY_COLUMNS = [
    "Employee Id",
    "Employee Name",
    "Date",
    "Productive Hours",
    "Neutral Time",
    "In Time",
    "Out Time",
    "Attendance",
    "Shift",
    "Email",
    "Role",
    "Contact Number",
    "Timezone",
    "Desktop",
    "Department",
    "Branch",
    "Report Head",
    "Total Time",
    "Working Hours",
    "Unproductive Time",
    "Idle Time",
    "Online Hours",
    "Away Time",
    "Productive Time(+Idle)",
    "Unproductive Time(+Idle)",
    "Neutral Time(+Idle)",
    "Report Head EmailId",
    "Total Time With Percentage",
    "Working Hours Percentage",
    "Productive Hours Percentage",
    "Unproductive Time With Percentage",
    "Neutral Time With Percentage",
    "Idle Time With Percentage",
    "Time Spent on Work Items",
    "Offline Time",
]

TRACKING_SUMMARY_COLUMNS = TIMECHAMP_LEGACY_COLUMNS + [
    "id",
    "timesheet_date",
    "total_time_seconds",
    "working_time_seconds",
    "productive_time_seconds",
    "idle_time_seconds",
    "unproductive_time_seconds",
    "neutral_time_seconds",
    "fetched_at",
    "updated_at",
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

        legacy_rows = []
        summary_rows = []
        for r in records:
            leg_row = [
                r.employee_id,
                r.employee_name,
                r.date_str,
                r.productive_hours,
                r.neutral_time,
                r.in_time,
                r.out_time,
                r.attendance,
                r.shift,
                r.email,
                r.role,
                r.contact_number,
                r.timezone,
                r.desktop,
                r.department,
                r.branch,
                r.report_head,
                r.total_time,
                r.working_hours,
                r.unproductive_time,
                r.idle_time,
                r.online_hours,
                r.away_time,
                r.productive_time_plus_idle,
                r.unproductive_time_plus_idle,
                r.neutral_time_plus_idle,
                r.report_head_email_id,
                r.total_time_with_percentage,
                r.working_hours_percentage,
                r.productive_hours_percentage,
                r.unproductive_time_with_percentage,
                r.neutral_time_with_percentage,
                r.idle_time_with_percentage,
                r.time_spent_on_work_items,
                r.offline_time,
            ]
            legacy_rows.append(leg_row)
            summary_rows.append(leg_row + [
                str(r.id),
                r.timesheet_date,
                r.total_time_seconds,
                r.working_time_seconds,
                r.productive_time_seconds,
                r.idle_time_seconds,
                r.unproductive_time_seconds,
                r.neutral_time_seconds,
                r.fetched_at,
                r.updated_at,
            ])

        # 1. Persist into COMMAND_CENTER.timechamp_tracking_summary
        summary_table = f"{settings.CLICKHOUSE_DB}.timechamp_tracking_summary"
        logger.info(f"Persisting batch of {len(summary_rows)} records into {summary_table}...")
        await self.ch.insert(table="timechamp_tracking_summary", data=summary_rows, column_names=TRACKING_SUMMARY_COLUMNS)

        # 2. Persist into legacy COMMAND_CENTER.TimeChamp with duplicate cleanup by date
        legacy_table = f"{settings.CLICKHOUSE_DB}.TimeChamp"
        try:
            dates = list({r.date_str for r in records if r.date_str})
            if dates:
                date_filter = ", ".join([f"'{d}'" for d in dates])
                await self.ch.command(f"ALTER TABLE {legacy_table} DELETE WHERE Date IN ({date_filter})")
            logger.info(f"Persisting batch of {len(legacy_rows)} records into {legacy_table}...")
            await self.ch.insert(table="TimeChamp", data=legacy_rows, column_names=TIMECHAMP_LEGACY_COLUMNS)
            logger.info(f"✓ Successfully synced {len(legacy_rows)} rows into {legacy_table}.")
        except Exception as leg_err:
            logger.warning(f"Notice while syncing to {legacy_table}: {leg_err}")

        logger.info(f"✓ Successfully inserted {len(records)} tracking summary rows into ClickHouse.")
        return len(records)

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
            conditions.append(f"`Employee Id` = '{filters.emp_id}'")
        if filters.department_name:
            conditions.append(f"Department = '{filters.department_name}'")
        if filters.start_date:
            conditions.append(f"timesheet_date >= '{filters.start_date.isoformat()}'")
        if filters.end_date:
            conditions.append(f"timesheet_date <= '{filters.end_date.isoformat()}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                id,
                `Employee Id` AS emp_id,
                Email AS username,
                `Employee Name` AS name,
                Department AS department_name,
                `Report Head` AS reporting_head_name,
                timesheet_date,
                Date AS timesheet_date_raw,
                `In Time` AS in_time,
                `Out Time` AS out_time,
                `Total Time` AS total_time,
                `Working Hours` AS working_hours,
                `Productive Hours` AS productive_hours,
                `Idle Time` AS idle_time,
                `Unproductive Time` AS unproductive_time,
                `Neutral Time` AS neutral_time,
                `Productive Hours Percentage` AS productive_percentage,
                `Working Hours Percentage` AS working_percentage,
                updated_at
            FROM {settings.CLICKHOUSE_DB}.timechamp_tracking_summary
            {where_clause}
            ORDER BY timesheet_date DESC, Department ASC, `Employee Id` ASC
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
