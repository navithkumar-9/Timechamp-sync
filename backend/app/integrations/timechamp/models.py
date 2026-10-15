from datetime import date, datetime
from typing import Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class TrackingSummaryRecord(BaseModel):
    """Domain model representing a normalized Timechamp tracking summary record in ClickHouse."""
    id: UUID = Field(default_factory=uuid4)
    emp_id: str = Field(..., description="Employee ID foreign key")
    username: str = Field(default="", description="Employee corporate email/login")
    name: str = Field(default="", description="Employee display name")
    department_name: str = Field(default="Unassigned", description="Department")
    reporting_head_name: str = Field(default="", description="Supervisor names")

    timesheet_date: date = Field(..., description="Normalized date (YYYY-MM-DD)")
    timesheet_date_raw: str = Field(default="", description="Original date string from API")

    start_date_time: Optional[datetime] = Field(None, description="Normalized clock-in timestamp")
    start_date_time_raw: str = Field(default="", description="Raw clock-in string")

    finish_date_time: Optional[datetime] = Field(None, description="Normalized clock-out timestamp")
    finish_date_time_raw: str = Field(default="", description="Raw clock-out string")

    total_time: int = Field(default=0, description="Total elapsed seconds")
    total_hours: float = Field(default=0.0, description="Total elapsed hours")
    working_time: int = Field(default=0, description="Active working seconds")
    working_hours: float = Field(default=0.0, description="Active working hours")
    productive_time: int = Field(default=0, description="Productive seconds")
    productive_hours: float = Field(default=0.0, description="Productive hours")
    idle_time: int = Field(default=0, description="Idle seconds")
    idle_hours: float = Field(default=0.0, description="Idle hours")
    unproductive_time: int = Field(default=0, description="Unproductive seconds")
    unproductive_hours: float = Field(default=0.0, description="Unproductive hours")
    neutral_time: int = Field(default=0, description="Neutral seconds")
    neutral_hours: float = Field(default=0.0, description="Neutral hours")

    idle_time_spent_on_productive_applications: int = 0
    idle_hours_spent_on_productive_applications: float = 0.0
    idle_time_spent_on_unproductive_applications: int = 0
    idle_hours_spent_on_unproductive_applications: float = 0.0
    idle_time_spent_on_neutral_applications: int = 0
    idle_hours_spent_on_neutral_applications: float = 0.0

    total_productive_time: int = 0
    total_productive_hours: float = 0.0
    total_unproductive_time: int = 0
    total_unproductive_hours: float = 0.0
    total_neutral_time: int = 0
    total_neutral_hours: float = 0.0

    total_activity_count: int = 0
    productive_activity_count: int = 0
    unproductive_activity_count: int = 0
    neutral_activity_count: int = 0

    productive_percentage: float = Field(default=0.0, description="Productive % of total time")
    working_percentage: float = Field(default=0.0, description="Working % of total time")

    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


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
