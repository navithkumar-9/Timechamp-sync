from typing import Any, Dict, List
from app.core.config import settings
from app.db.clickhouse import ClickHouseManager, ch_manager
from app.schemas.timechamp import EmployeeRecord, TimeChampFilter, TimeChampRecord


class TimeChampService:
    def __init__(self, ch: ClickHouseManager = ch_manager):
        self.ch = ch

    async def get_timechamp_records(self, filter_params: TimeChampFilter) -> List[Dict[str, Any]]:
        """Query TimeChamp attendance and productivity logs from ClickHouse."""
        conditions = []
        if filter_params.employee_id:
            conditions.append(f"`Employee Id` = '{filter_params.employee_id}'")
        if filter_params.department:
            conditions.append(f"`Department` = '{filter_params.department}'")
        if filter_params.date:
            conditions.append(f"`Date` = '{filter_params.date}'")
        if filter_params.attendance:
            conditions.append(f"`Attendance` = '{filter_params.attendance}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT
                `Employee Id`,
                `Employee Name`,
                `Date`,
                `Productive Hours`,
                `Working Hours`,
                `Total Time`,
                `Idle Time`,
                `Attendance`,
                `Shift`,
                `Email`,
                `Role`,
                `Department`,
                `Branch`,
                `Report Head`
            FROM {settings.CLICKHOUSE_DB}.TimeChamp
            {where_clause}
            ORDER BY `Date` DESC
            LIMIT {filter_params.limit} OFFSET {filter_params.offset}
        """

        rows = await self.ch.query(query)
        return [TimeChampRecord(**row).model_dump(by_alias=False) for row in rows]

    async def get_employees(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Query active employees from COMMAND_CENTER.Employee_list."""
        query = f"""
            SELECT
                id,
                Status,
                Role,
                Designation,
                Employee_ID,
                `Employee Name`,
                Department,
                First_Line_Supervisor,
                official_mail
            FROM {settings.CLICKHOUSE_DB}.Employee_list
            ORDER BY id ASC
            LIMIT {limit} OFFSET {offset}
        """
        rows = await self.ch.query(query)
        return [EmployeeRecord(**row).model_dump(by_alias=False) for row in rows]

    async def get_department_attendance_summary(self) -> List[Dict[str, Any]]:
        """Get aggregate counts of attendance status by department."""
        query = f"""
            SELECT
                Department,
                Attendance,
                count() as count
            FROM {settings.CLICKHOUSE_DB}.TimeChamp
            WHERE Department IS NOT NULL AND Attendance IS NOT NULL
            GROUP BY Department, Attendance
            ORDER BY Department, count DESC
        """
        return await self.ch.query(query)


timechamp_service = TimeChampService()


def get_timechamp_service() -> TimeChampService:
    return timechamp_service
