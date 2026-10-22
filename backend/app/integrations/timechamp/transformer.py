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

    def _format_date_b_d_Y(self, d: date) -> str:
        """Format date as '%b-%d-%Y' (e.g., 'Oct-22-2026') required by downstream ClickHouse views."""
        return d.strftime("%b-%d-%Y")

    def _seconds_to_hms(self, seconds: int) -> str:
        """Convert integer seconds into 'HH:MM:SS' string format."""
        sec = max(0, int(seconds or 0))
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _extract_time_hh_mm(self, raw_str: Optional[str]) -> str:
        """Extract 'HH:MM' clock time from timestamp string."""
        if not raw_str:
            return ""
        parts = raw_str.strip().split()
        if len(parts) >= 2:
            time_part = parts[1]
            t_parts = time_part.split(":")
            if len(t_parts) >= 2:
                try:
                    hh = int(t_parts[0])
                    mm = int(t_parts[1])
                    return f"{hh:02d}:{mm:02d}"
                except ValueError:
                    pass
        return ""

    def _normalize_out_time_24h(self, in_time: str, out_raw: Optional[str]) -> str:
        """Ensure out time is in 24-hour 'HH:MM' format, resolving 12-hour PM rollovers."""
        out_t = self._extract_time_hh_mm(out_raw)
        if not out_t or not in_time:
            return out_t
        try:
            in_hh, in_mm = [int(x) for x in in_time.split(":")]
            out_hh, out_mm = [int(x) for x in out_t.split(":")]
            if in_hh > out_hh and out_hh < 12:
                out_hh += 12
            return f"{out_hh:02d}:{out_mm:02d}"
        except Exception:
            return out_t

    def _compute_attendance(self, work_sec: int, prod_sec: int) -> str:
        """Derive standard attendance status (Full day, Half day, Absent)."""
        ref_sec = max(work_sec, prod_sec)
        if ref_sec >= 27900:  # ~7h 45m / 8h
            return "Full day"
        elif ref_sec >= 14400:  # 4h
            return "Half day"
        elif ref_sec > 0:
            return "Half day"
        return "Absent"

    def _compute_pct_str(self, val: int, base: int) -> str:
        """Format percentage string (e.g., '92%')."""
        if base <= 0:
            return "0%"
        pct = round((val / base) * 100)
        return f"{pct}%"

    def transform_record(self, raw: Dict[str, Any]) -> TrackingSummaryRecord:
        """Converts raw JSON dictionary from Timechamp API into a strongly-typed TrackingSummaryRecord with all 35 legacy columns."""
        raw_timesheet_date = str(raw.get("timesheetDate") or "")
        parsed_date = self._parse_timesheet_date(raw_timesheet_date)
        formatted_date = self._format_date_b_d_Y(parsed_date)

        raw_start = str(raw.get("startDateTime") or "")
        in_time = self._extract_time_hh_mm(raw_start) if raw_start else None

        raw_finish = str(raw.get("finishDateTime") or "")
        out_time = self._normalize_out_time_24h(in_time or "", raw_finish) if raw_finish else None

        total_time = max(0, int(raw.get("totalTime") or 0))
        working_time = max(0, int(raw.get("workingTime") or 0))
        productive_time = max(0, int(raw.get("productiveTime") or 0))
        idle_time = max(0, int(raw.get("idleTime") or 0))
        unproductive_time = max(0, int(raw.get("unproductiveTime") or 0))
        neutral_time = max(0, int(raw.get("neutralTime") or 0))

        idle_productive = max(0, int(raw.get("idleTimeSpentOnProductiveApplications") or 0))
        idle_unproductive = max(0, int(raw.get("idleTimeSpentOnUnroductiveApplications") or 0))
        idle_neutral = max(0, int(raw.get("idleTimeSpentOnNeutralApplications") or 0))

        away_time = max(0, total_time - working_time)
        online_time = max(0, working_time + idle_time)
        offline_time = max(0, total_time - online_time)

        prod_plus_idle = productive_time + idle_productive
        unprod_plus_idle = unproductive_time + idle_unproductive
        neut_plus_idle = neutral_time + idle_neutral

        attendance = self._compute_attendance(working_time, productive_time)

        # Standard 9-hour workday baseline (32400 seconds)
        total_time_pct = self._compute_pct_str(total_time, 32400)
        working_hours_pct = self._compute_pct_str(working_time, 32400)
        prod_hours_pct = self._compute_pct_str(productive_time, working_time or 1)
        unprod_pct = self._compute_pct_str(unproductive_time, working_time or 1)
        neut_pct = self._compute_pct_str(neutral_time, working_time or 1)
        idle_pct = self._compute_pct_str(idle_time, total_time or 1)

        emp_id = str(raw.get("empID") or "UNKNOWN").strip()
        emp_name = str(raw.get("name") or "").strip()
        email = str(raw.get("username") or "").strip()
        dept = str(raw.get("departmentName") or "Unassigned").strip()
        rep_head = str(raw.get("reportingHeadName") or "").strip()
        rep_email = str(raw.get("reportingHeadEmailId") or raw.get("reportingHeadEmail") or "").strip()

        return TrackingSummaryRecord(
            # 35 Legacy columns matching COMMAND_CENTER.TimeChamp exactly
            **{
                "Employee Id": emp_id,
                "Employee Name": emp_name,
                "Date": formatted_date,
                "Productive Hours": self._seconds_to_hms(productive_time),
                "Neutral Time": self._seconds_to_hms(neutral_time),
                "In Time": in_time,
                "Out Time": out_time,
                "Attendance": attendance,
                "Shift": str(raw.get("shift") or "08:00 IST - 19:30 IST").strip(),
                "Email": email,
                "Role": str(raw.get("role") or "User").strip(),
                "Contact Number": raw.get("contactNumber") or raw.get("phoneNumber"),
                "Timezone": raw.get("timezone"),
                "Desktop": raw.get("desktop") or raw.get("desktopName"),
                "Department": dept,
                "Branch": str(raw.get("branch") or "AUTOSherpa Systems").strip(),
                "Report Head": rep_head,
                "Total Time": self._seconds_to_hms(total_time),
                "Working Hours": self._seconds_to_hms(working_time),
                "Unproductive Time": self._seconds_to_hms(unproductive_time),
                "Idle Time": self._seconds_to_hms(idle_time),
                "Online Hours": self._seconds_to_hms(online_time),
                "Away Time": self._seconds_to_hms(away_time),
                "Productive Time(+Idle)": self._seconds_to_hms(prod_plus_idle),
                "Unproductive Time(+Idle)": self._seconds_to_hms(unprod_plus_idle),
                "Neutral Time(+Idle)": self._seconds_to_hms(neut_plus_idle),
                "Report Head EmailId": rep_email,
                "Total Time With Percentage": total_time_pct,
                "Working Hours Percentage": working_hours_pct,
                "Productive Hours Percentage": prod_hours_pct,
                "Unproductive Time With Percentage": unprod_pct,
                "Neutral Time With Percentage": neut_pct,
                "Idle Time With Percentage": idle_pct,
                "Time Spent on Work Items": "00:00:00",
                "Offline Time": self._seconds_to_hms(offline_time),
                # Additional analytics fields
                "id": uuid4(),
                "timesheet_date": parsed_date,
                "total_time_seconds": total_time,
                "working_time_seconds": working_time,
                "productive_time_seconds": productive_time,
                "idle_time_seconds": idle_time,
                "unproductive_time_seconds": unproductive_time,
                "neutral_time_seconds": neutral_time,
                "fetched_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
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
