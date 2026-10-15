from typing import Optional
from pydantic import BaseModel, Field


class TimeChampRecord(BaseModel):
    employee_id: Optional[str] = Field(None, alias="Employee Id")
    employee_name: Optional[str] = Field(None, alias="Employee Name")
    date: Optional[str] = Field(None, alias="Date")
    productive_hours: Optional[str] = Field(None, alias="Productive Hours")
    working_hours: Optional[str] = Field(None, alias="Working Hours")
    total_time: Optional[str] = Field(None, alias="Total Time")
    idle_time: Optional[str] = Field(None, alias="Idle Time")
    attendance: Optional[str] = Field(None, alias="Attendance")
    shift: Optional[str] = Field(None, alias="Shift")
    email: Optional[str] = Field(None, alias="Email")
    role: Optional[str] = Field(None, alias="Role")
    department: Optional[str] = Field(None, alias="Department")
    branch: Optional[str] = Field(None, alias="Branch")
    report_head: Optional[str] = Field(None, alias="Report Head")

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }


class TimeChampFilter(BaseModel):
    employee_id: Optional[str] = None
    department: Optional[str] = None
    date: Optional[str] = None
    attendance: Optional[str] = None
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class EmployeeRecord(BaseModel):
    id: Optional[int] = None
    status: Optional[str] = Field(None, alias="Status")
    role: Optional[str] = Field(None, alias="Role")
    designation: Optional[str] = Field(None, alias="Designation")
    employee_id: Optional[str] = Field(None, alias="Employee_ID")
    employee_name: Optional[str] = Field(None, alias="Employee Name")
    department: Optional[str] = Field(None, alias="Department")
    first_line_supervisor: Optional[str] = Field(None, alias="First_Line_Supervisor")
    official_mail: Optional[str] = Field(None, alias="official_mail")

    model_config = {
        "populate_by_name": True,
        "extra": "ignore",
    }
