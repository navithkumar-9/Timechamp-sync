from datetime import date, datetime
from typing import Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class TrackingSummaryRecord(BaseModel):
    """Domain model representing a normalized Timechamp tracking summary record in ClickHouse.
    
    Contains all 35 legacy columns matching COMMAND_CENTER.TimeChamp schema exactly,
    plus typed fields for high-performance indexing and aggregation.
    """
    # 35 Legacy TimeChamp columns
    employee_id: str = Field(..., alias="Employee Id", description="Employee ID foreign key")
    employee_name: str = Field(default="", alias="Employee Name", description="Employee display name")
    date_str: str = Field(..., alias="Date", description="Formatted timesheet date (%b-%d-%Y)")
    productive_hours: str = Field(default="00:00:00", alias="Productive Hours", description="Productive hours (HH:MM:SS)")
    neutral_time: str = Field(default="00:00:00", alias="Neutral Time", description="Neutral time (HH:MM:SS)")
    in_time: Optional[str] = Field(None, alias="In Time", description="Clock-in time (HH:MM)")
    out_time: Optional[str] = Field(None, alias="Out Time", description="Clock-out time (HH:MM)")
    attendance: str = Field(default="Absent", alias="Attendance", description="Attendance status")
    shift: str = Field(default="08:00 IST - 19:30 IST", alias="Shift", description="Shift schedule")
    email: str = Field(default="", alias="Email", description="Employee corporate email")
    role: str = Field(default="User", alias="Role", description="User role")
    contact_number: Optional[str] = Field(None, alias="Contact Number", description="Contact phone")
    timezone: Optional[str] = Field(None, alias="Timezone", description="User timezone")
    desktop: Optional[str] = Field(None, alias="Desktop", description="Workstation name")
    department: str = Field(default="Unassigned", alias="Department", description="Department name")
    branch: str = Field(default="AUTOSherpa Systems", alias="Branch", description="Branch location")
    report_head: str = Field(default="", alias="Report Head", description="Supervisor name(s)")
    total_time: str = Field(default="00:00:00", alias="Total Time", description="Total elapsed time (HH:MM:SS)")
    working_hours: str = Field(default="00:00:00", alias="Working Hours", description="Working time (HH:MM:SS)")
    unproductive_time: str = Field(default="00:00:00", alias="Unproductive Time", description="Unproductive time (HH:MM:SS)")
    idle_time: str = Field(default="00:00:00", alias="Idle Time", description="Idle time (HH:MM:SS)")
    online_hours: str = Field(default="00:00:00", alias="Online Hours", description="Online time (HH:MM:SS)")
    away_time: str = Field(default="00:00:00", alias="Away Time", description="Away time (HH:MM:SS)")
    productive_time_plus_idle: str = Field(default="00:00:00", alias="Productive Time(+Idle)")
    unproductive_time_plus_idle: str = Field(default="00:00:00", alias="Unproductive Time(+Idle)")
    neutral_time_plus_idle: str = Field(default="00:00:00", alias="Neutral Time(+Idle)")
    report_head_email_id: str = Field(default="", alias="Report Head EmailId")
    total_time_with_percentage: str = Field(default="0%", alias="Total Time With Percentage")
    working_hours_percentage: str = Field(default="0%", alias="Working Hours Percentage")
    productive_hours_percentage: str = Field(default="0%", alias="Productive Hours Percentage")
    unproductive_time_with_percentage: str = Field(default="0%", alias="Unproductive Time With Percentage")
    neutral_time_with_percentage: str = Field(default="0%", alias="Neutral Time With Percentage")
    idle_time_with_percentage: str = Field(default="0%", alias="Idle Time With Percentage")
    time_spent_on_work_items: str = Field(default="00:00:00", alias="Time Spent on Work Items")
    offline_time: str = Field(default="00:00:00", alias="Offline Time")

    # Technical & analytics columns
    id: UUID = Field(default_factory=uuid4)
    timesheet_date: date = Field(..., description="Normalized date (YYYY-MM-DD)")
    total_time_seconds: int = 0
    working_time_seconds: int = 0
    productive_time_seconds: int = 0
    idle_time_seconds: int = 0
    unproductive_time_seconds: int = 0
    neutral_time_seconds: int = 0
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }

    # Backward compatibility properties
    @property
    def emp_id(self) -> str:
        return self.employee_id

    @property
    def name(self) -> str:
        return self.employee_name

    @property
    def username(self) -> str:
        return self.email

    @property
    def department_name(self) -> str:
        return self.department

    @property
    def reporting_head_name(self) -> str:
        return self.report_head


class SyncHistoryRecord(BaseModel):
    """Audit log entry for tracking sync executions."""
    sync_id: UUID = Field(default_factory=uuid4)
    sync_type: str = Field(default="MANUAL", description="MANUAL, SCHEDULED_CRON, KAFKA_TRIGGER")
    status: str = Field(default="SUCCESS", description="SUCCESS, FAILED, PARTIAL")
    records_fetched: int = 0
    records_inserted: int = 0
    duration_ms: float = 0.0
    error_message: str = ""
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime = Field(default_factory=datetime.utcnow)


class SyncResult(BaseModel):
    """Result object returned by the sync pipeline."""
    success: bool
    sync_id: str
    records_fetched: int
    records_inserted: int
    duration_ms: float
    message: str
    error: Optional[str] = None


class TrackingSummaryFilter(BaseModel):
    emp_id: Optional[str] = None
    department_name: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: int = Field(default=100, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)
