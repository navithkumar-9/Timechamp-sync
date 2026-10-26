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

        target_table = f"{settings.CLICKHOUSE_DB}.TimeChamp"

        # Deduplicate: delete existing records for these dates in TimeChamp prior to insert
        dates = list({r.date_str for r in records if r.date_str})
        if dates:
            date_filter = ", ".join([f"'{d}'" for d in dates])
            delete_cmd = f"ALTER TABLE {target_table} DELETE WHERE Date IN ({date_filter})"
            logger.info(f"Purging existing records in {target_table} for dates: {dates} prior to insert...")
            try:
                client = self.ch.connect()
                client.command(delete_cmd, settings={"mutations_sync": 1})
            except Exception as del_err:
                logger.warning(f"Notice while clearing existing date records from {target_table}: {del_err}")

        # Persist into COMMAND_CENTER.TimeChamp
        logger.info(f"Persisting batch of {len(legacy_rows)} records into {target_table}...")
        await self.ch.insert(table="TimeChamp", data=legacy_rows, column_names=TIMECHAMP_LEGACY_COLUMNS)

        logger.info(f"✓ Successfully inserted {len(legacy_rows)} rows into {target_table}.")
        return len(legacy_rows)

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
        conditions = ["Date IS NOT NULL AND Date != ''"]
        if filters.emp_id:
            conditions.append(f"`Employee Id` = '{filters.emp_id}'")
        if filters.department_name:
            conditions.append(f"Department = '{filters.department_name}'")
        if filters.start_date:
            conditions.append(f"toDate(parseDateTime(Date, '%b-%d-%Y')) >= '{filters.start_date.isoformat()}'")
        if filters.end_date:
            conditions.append(f"toDate(parseDateTime(Date, '%b-%d-%Y')) <= '{filters.end_date.isoformat()}'")

        where_clause = f"WHERE {' AND '.join(conditions)}"
        query = f"""
            SELECT
                generateUUIDv4() AS id,
                `Employee Id` AS emp_id,
                Email AS username,
                `Employee Name` AS name,
                Department AS department_name,
                `Report Head` AS reporting_head_name,
                toDate(parseDateTime(Date, '%b-%d-%Y')) AS timesheet_date,
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
                now() AS updated_at
            FROM {settings.CLICKHOUSE_DB}.TimeChamp
            {where_clause}
            ORDER BY timesheet_date DESC, Department ASC, `Employee Id` ASC
            LIMIT {filters.limit} OFFSET {filters.offset}
        """
        return await self.ch.query(query)

    async def get_latest_synced_date(self) -> Optional[date]:
        """Fetch the most recent timesheet_date stored in ClickHouse TimeChamp."""
        query = f"""
            SELECT max(toDate(parseDateTime(Date, '%b-%d-%Y'))) as max_date 
            FROM {settings.CLICKHOUSE_DB}.TimeChamp 
            WHERE Date IS NOT NULL AND Date != ''
        """
        try:
            rows = await self.ch.query(query)
            if rows and rows[0].get("max_date"):
                val = rows[0]["max_date"]
                if isinstance(val, date):
                    return val
                elif isinstance(val, str):
                    return datetime.strptime(val, "%Y-%m-%d").date()
        except Exception as e:
            logger.warning(f"Could not retrieve latest date from ClickHouse TimeChamp: {e}")
        return None


default_repository = ClickHouseTrackingSummaryRepository()


def get_repository() -> ITrackingSummaryRepository:
    return default_repository
