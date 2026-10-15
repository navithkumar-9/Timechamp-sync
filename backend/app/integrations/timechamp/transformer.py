from datetime import date, datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4
from app.core.logger import logger
from app.integrations.timechamp.interfaces import ITrackingDataTransformer
from app.integrations.timechamp.models import TrackingSummaryRecord


class TrackingDataTransformer(ITrackingDataTransformer):
    """Robust data transformation and normalization service adhering to SRP."""

    def _parse_timesheet_date(self, raw_date_str: Optional[str]) -> date:
        """Parse raw timesheet date string (e.g., '14-Oct-26', '2026-10-14') into a Python date."""
        if not raw_date_str:
            return date.today()

        raw = raw_date_str.strip()
        formats = [
            "%d-%b-%y",  # 14-Oct-26
            "%d-%b-%Y",  # 14-Oct-2026
            "%Y-%m-%d",  # 2026-10-14
            "%d/%m/%Y",  # 14/10/2026
            "%d-%m-%Y",  # 14-10-2026
        ]

        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue

        logger.warning(f"Could not parse timesheetDate '{raw}', falling back to current date.")
        return date.today()

    def _parse_datetime(self, raw_dt_str: Optional[str]) -> Optional[datetime]:
        """Parse timestamp string (e.g. '14/10/2026 09:54') into UTC datetime."""
        if not raw_dt_str:
            return None

        raw = raw_dt_str.strip()
        formats = [
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d-%b-%y %H:%M",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue

        return None

    def transform_record(self, raw: Dict[str, Any]) -> TrackingSummaryRecord:
        """Converts raw JSON dictionary from Timechamp API into a strongly-typed TrackingSummaryRecord."""
        raw_timesheet_date = str(raw.get("timesheetDate") or "")
        parsed_date = self._parse_timesheet_date(raw_timesheet_date)

        raw_start = str(raw.get("startDateTime") or "")
        parsed_start = self._parse_datetime(raw_start)

        raw_finish = str(raw.get("finishDateTime") or "")
        parsed_finish = self._parse_datetime(raw_finish)

        total_time = max(0, int(raw.get("totalTime") or 0))
        working_time = max(0, int(raw.get("workingTime") or 0))
        productive_time = max(0, int(raw.get("productiveTime") or 0))
        idle_time = max(0, int(raw.get("idleTime") or 0))
        unproductive_time = max(0, int(raw.get("unproductiveTime") or 0))
        neutral_time = max(0, int(raw.get("neutralTime") or 0))

        idle_productive = max(0, int(raw.get("idleTimeSpentOnProductiveApplications") or 0))
        idle_unproductive = max(0, int(raw.get("idleTimeSpentOnUnroductiveApplications") or 0))
        idle_neutral = max(0, int(raw.get("idleTimeSpentOnNeutralApplications") or 0))

        tot_productive = max(0, int(raw.get("totalProductiveTime") or 0))
        tot_unproductive = max(0, int(raw.get("totalUnProductiveTime") or 0))
        tot_neutral = max(0, int(raw.get("totalNeutralTime") or 0))

        def _to_hours(sec: int) -> float:
            return round(sec / 3600.0, 2)

        productive_pct = round((productive_time / total_time) * 100.0, 2) if total_time > 0 else 0.0
        working_pct = round((working_time / total_time) * 100.0, 2) if total_time > 0 else 0.0

        return TrackingSummaryRecord(
            id=uuid4(),
            emp_id=str(raw.get("empID") or "UNKNOWN").strip(),
            username=str(raw.get("username") or "").strip(),
            name=str(raw.get("name") or "").strip(),
            department_name=str(raw.get("departmentName") or "Unassigned").strip(),
            reporting_head_name=str(raw.get("reportingHeadName") or "").strip(),
            timesheet_date=parsed_date,
            timesheet_date_raw=raw_timesheet_date,
            start_date_time=parsed_start,
            start_date_time_raw=raw_start,
            finish_date_time=parsed_finish,
            finish_date_time_raw=raw_finish,
            total_time=total_time,
            total_hours=_to_hours(total_time),
            working_time=working_time,
            working_hours=_to_hours(working_time),
            productive_time=productive_time,
            productive_hours=_to_hours(productive_time),
            idle_time=idle_time,
            idle_hours=_to_hours(idle_time),
            unproductive_time=unproductive_time,
            unproductive_hours=_to_hours(unproductive_time),
            neutral_time=neutral_time,
            neutral_hours=_to_hours(neutral_time),
            idle_time_spent_on_productive_applications=idle_productive,
            idle_hours_spent_on_productive_applications=_to_hours(idle_productive),
            idle_time_spent_on_unproductive_applications=idle_unproductive,
            idle_hours_spent_on_unproductive_applications=_to_hours(idle_unproductive),
            idle_time_spent_on_neutral_applications=idle_neutral,
            idle_hours_spent_on_neutral_applications=_to_hours(idle_neutral),
            total_productive_time=tot_productive,
            total_productive_hours=_to_hours(tot_productive),
            total_unproductive_time=tot_unproductive,
            total_unproductive_hours=_to_hours(tot_unproductive),
            total_neutral_time=tot_neutral,
            total_neutral_hours=_to_hours(tot_neutral),
            total_activity_count=max(0, int(raw.get("totalActivityCount") or 0)),
            productive_activity_count=max(0, int(raw.get("productiveActivityCount") or 0)),
            unproductive_activity_count=max(0, int(raw.get("unproductiveActivityCount") or 0)),
            neutral_activity_count=max(0, int(raw.get("neutralActivityCount") or 0)),
            productive_percentage=productive_pct,
            working_percentage=working_pct,
            fetched_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )

    def transform_batch(self, raw_list: List[Dict[str, Any]]) -> List[TrackingSummaryRecord]:
        results: List[TrackingSummaryRecord] = []
        for raw in raw_list:
            try:
                record = self.transform_record(raw)
                results.append(record)
            except Exception as e:
                logger.error(f"Error transforming record for empID '{raw.get('empID')}': {e}")
        return results


default_transformer = TrackingDataTransformer()


def get_transformer() -> ITrackingDataTransformer:
    return default_transformer
