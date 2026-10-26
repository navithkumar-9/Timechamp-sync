-- ==============================================================================
-- Migration / Ingestion Script: Payroll Cycle 2026-10 (26-Sep-2026 to 25-Oct-2026)
-- Target Table: COMMAND_CENTER.TimeChamp
-- Source Table: COMMAND_CENTER.timechamp_tracking_summary
-- ==============================================================================

-- Step 1: Idempotent cleanup of any partial or duplicate data in target cycle
ALTER TABLE COMMAND_CENTER.TimeChamp DELETE WHERE 
    toDate(parseDateTime(Date, '%b-%d-%Y')) >= '2026-09-26' 
    AND toDate(parseDateTime(Date, '%b-%d-%Y')) <= '2026-10-25';

-- Step 2: Insert complete 35-column normalized tracking records
INSERT INTO COMMAND_CENTER.TimeChamp (
    `Employee Id`,
    `Employee Name`,
    `Date`,
    `Productive Hours`,
    `Neutral Time`,
    `In Time`,
    `Out Time`,
    `Attendance`,
    `Shift`,
    `Email`,
    `Role`,
    `Contact Number`,
    `Timezone`,
    `Desktop`,
    `Department`,
    `Branch`,
    `Report Head`,
    `Total Time`,
    `Working Hours`,
    `Unproductive Time`,
    `Idle Time`,
    `Online Hours`,
    `Away Time`,
    `Productive Time(+Idle)`,
    `Unproductive Time(+Idle)`,
    `Neutral Time(+Idle)`,
    `Report Head EmailId`,
    `Total Time With Percentage`,
    `Working Hours Percentage`,
    `Productive Hours Percentage`,
    `Unproductive Time With Percentage`,
    `Neutral Time With Percentage`,
    `Idle Time With Percentage`,
    `Time Spent on Work Items`,
    `Offline Time`
)
SELECT
    `Employee Id`,
    `Employee Name`,
    `Date`,
    `Productive Hours`,
    `Neutral Time`,
    `In Time`,
    `Out Time`,
    `Attendance`,
    `Shift`,
    `Email`,
    `Role`,
    `Contact Number`,
    `Timezone`,
    `Desktop`,
    `Department`,
    `Branch`,
    `Report Head`,
    `Total Time`,
    `Working Hours`,
    `Unproductive Time`,
    `Idle Time`,
    `Online Hours`,
    `Away Time`,
    `Productive Time(+Idle)`,
    `Unproductive Time(+Idle)`,
    `Neutral Time(+Idle)`,
    `Report Head EmailId`,
    `Total Time With Percentage`,
    `Working Hours Percentage`,
    `Productive Hours Percentage`,
    `Unproductive Time With Percentage`,
    `Neutral Time With Percentage`,
    `Idle Time With Percentage`,
    `Time Spent on Work Items`,
    `Offline Time`
FROM COMMAND_CENTER.timechamp_tracking_summary
WHERE timesheet_date >= '2026-09-26' AND timesheet_date <= '2026-10-25';
