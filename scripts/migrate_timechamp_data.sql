-- Migration: Copy historical data from COMMAND_CENTER.TimeChamp to COMMAND_CENTER.timechamp_tracking_summary
-- Date Range: 24-09-2026 to 08-10-2026

-- Step 1: Clean any existing records for the target window to prevent duplicates
ALTER TABLE COMMAND_CENTER.timechamp_tracking_summary 
DELETE WHERE timesheet_date >= '2026-09-24' AND timesheet_date <= '2026-10-08';

-- Step 2: Insert 1:1 mapped columns from TimeChamp to timechamp_tracking_summary
INSERT INTO COMMAND_CENTER.timechamp_tracking_summary (
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
    `Offline Time`,
    id,
    timesheet_date,
    total_time_seconds,
    working_time_seconds,
    productive_time_seconds,
    idle_time_seconds,
    unproductive_time_seconds,
    neutral_time_seconds,
    fetched_at,
    updated_at
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
    `Offline Time`,
    generateUUIDv4() AS id,
    toDate(parseDateTime(Date, '%b-%d-%Y')) AS timesheet_date,
    if(length(splitByChar(':', coalesce(`Total Time`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Total Time`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Total Time`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Total Time`, ''))[3]), 0) AS total_time_seconds,
    if(length(splitByChar(':', coalesce(`Working Hours`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Working Hours`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Working Hours`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Working Hours`, ''))[3]), 0) AS working_time_seconds,
    if(length(splitByChar(':', coalesce(`Productive Hours`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Productive Hours`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Productive Hours`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Productive Hours`, ''))[3]), 0) AS productive_time_seconds,
    if(length(splitByChar(':', coalesce(`Idle Time`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Idle Time`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Idle Time`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Idle Time`, ''))[3]), 0) AS idle_time_seconds,
    if(length(splitByChar(':', coalesce(`Unproductive Time`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Unproductive Time`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Unproductive Time`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Unproductive Time`, ''))[3]), 0) AS unproductive_time_seconds,
    if(length(splitByChar(':', coalesce(`Neutral Time`, ''))) = 3,
       toUInt32OrZero(splitByChar(':', coalesce(`Neutral Time`, ''))[1]) * 3600 +
       toUInt32OrZero(splitByChar(':', coalesce(`Neutral Time`, ''))[2]) * 60 +
       toUInt32OrZero(splitByChar(':', coalesce(`Neutral Time`, ''))[3]), 0) AS neutral_time_seconds,
    now() AS fetched_at,
    now64(3) AS updated_at
FROM COMMAND_CENTER.TimeChamp
WHERE toDate(parseDateTime(Date, '%b-%d-%Y')) >= '2026-09-24' 
  AND toDate(parseDateTime(Date, '%b-%d-%Y')) <= '2026-10-08';
