import clickhouse_connect
from app.core.config import settings
from app.core.logger import logger


MIGRATION_DDLS = [
    # 1. Ensure target database exists
    f"CREATE DATABASE IF NOT EXISTS {settings.CLICKHOUSE_DB}",

    # 2. Main Metrics Table (MergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.metrics (
        id UUID DEFAULT generateUUIDv4(),
        tenant_id String,
        device_id String,
        metric_name LowCardinality(String),
        value Float64,
        unit LowCardinality(String),
        tags Map(String, String),
        timestamp DateTime64(3, 'UTC') DEFAULT now64(3),
        created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (tenant_id, metric_name, timestamp, id)
    SETTINGS index_granularity = 8192;
    """,

    # 3. System Events Table (MergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.system_events (
        event_id UUID DEFAULT generateUUIDv4(),
        event_type LowCardinality(String),
        source LowCardinality(String),
        payload String,
        status LowCardinality(String),
        event_time DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(event_time)
    ORDER BY (event_type, event_time, event_id)
    SETTINGS index_granularity = 8192;
    """,

    # 4. Hourly Aggregations Table (SummingMergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.metrics_hourly (
        hour_bucket DateTime,
        tenant_id String,
        metric_name LowCardinality(String),
        metric_count UInt64,
        metric_total Float64,
        max_value Float64,
        min_value Float64
    ) ENGINE = SummingMergeTree((metric_count, metric_total))
    PARTITION BY toYYYYMM(hour_bucket)
    ORDER BY (tenant_id, metric_name, hour_bucket);
    """,

    # 5. Timechamp Tracking Summary (ReplacingMergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.timechamp_tracking_summary (
        `Employee Id` Nullable(String) COMMENT 'Employee ID referencing COMMAND_CENTER.Employee_list(Employee_ID)',
        `Employee Name` Nullable(String) COMMENT 'Employee full name',
        `Date` Nullable(String) COMMENT 'Formatted timesheet date (%b-%d-%Y)',
        `Productive Hours` Nullable(String) COMMENT 'Productive hours (HH:MM:SS)',
        `Neutral Time` Nullable(String) COMMENT 'Neutral time (HH:MM:SS)',
        `In Time` Nullable(String) COMMENT 'Clock-in time (HH:MM)',
        `Out Time` Nullable(String) COMMENT 'Clock-out time (HH:MM)',
        `Attendance` Nullable(String) COMMENT 'Attendance status (Full day, Half day, Absent)',
        `Shift` Nullable(String) COMMENT 'Assigned shift schedule',
        `Email` Nullable(String) COMMENT 'Corporate email address',
        `Role` Nullable(String) COMMENT 'User role',
        `Contact Number` Nullable(String) COMMENT 'Contact phone number',
        `Timezone` Nullable(String) COMMENT 'User timezone',
        `Desktop` Nullable(String) COMMENT 'Workstation device identifier',
        `Department` Nullable(String) COMMENT 'Employee department',
        `Branch` Nullable(String) COMMENT 'Company branch location',
        `Report Head` Nullable(String) COMMENT 'Reporting manager(s)',
        `Total Time` Nullable(String) COMMENT 'Total elapsed time (HH:MM:SS)',
        `Working Hours` Nullable(String) COMMENT 'Working time (HH:MM:SS)',
        `Unproductive Time` Nullable(String) COMMENT 'Unproductive time (HH:MM:SS)',
        `Idle Time` Nullable(String) COMMENT 'Idle time (HH:MM:SS)',
        `Online Hours` Nullable(String) COMMENT 'Online time (HH:MM:SS)',
        `Away Time` Nullable(String) COMMENT 'Away time (HH:MM:SS)',
        `Productive Time(+Idle)` Nullable(String) COMMENT 'Productive time including idle on productive apps (HH:MM:SS)',
        `Unproductive Time(+Idle)` Nullable(String) COMMENT 'Unproductive time including idle on unproductive apps (HH:MM:SS)',
        `Neutral Time(+Idle)` Nullable(String) COMMENT 'Neutral time including idle on neutral apps (HH:MM:SS)',
        `Report Head EmailId` Nullable(String) COMMENT 'Reporting manager email(s)',
        `Total Time With Percentage` Nullable(String) COMMENT 'Total time percentage (e.g. 100%)',
        `Working Hours Percentage` Nullable(String) COMMENT 'Working hours percentage (e.g. 92%)',
        `Productive Hours Percentage` Nullable(String) COMMENT 'Productive percentage (e.g. 100%)',
        `Unproductive Time With Percentage` Nullable(String) COMMENT 'Unproductive percentage (e.g. 0%)',
        `Neutral Time With Percentage` Nullable(String) COMMENT 'Neutral percentage (e.g. 0%)',
        `Idle Time With Percentage` Nullable(String) COMMENT 'Idle percentage (e.g. 16%)',
        `Time Spent on Work Items` Nullable(String) COMMENT 'Time spent on work items (HH:MM:SS)',
        `Offline Time` Nullable(String) COMMENT 'Offline time (HH:MM:SS)',
        id UUID DEFAULT generateUUIDv4(),
        timesheet_date Date,
        total_time_seconds UInt32 DEFAULT 0,
        working_time_seconds UInt32 DEFAULT 0,
        productive_time_seconds UInt32 DEFAULT 0,
        idle_time_seconds UInt32 DEFAULT 0,
        unproductive_time_seconds UInt32 DEFAULT 0,
        neutral_time_seconds UInt32 DEFAULT 0,
        fetched_at DateTime DEFAULT now(),
        updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(timesheet_date)
    ORDER BY (timesheet_date, ifNull(`Employee Id`, ''))
    SETTINGS index_granularity = 8192;
    """,

    # 6. Timechamp Sync History (MergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.timechamp_sync_history (
        sync_id UUID DEFAULT generateUUIDv4(),
        sync_type LowCardinality(String),
        status LowCardinality(String),
        records_fetched UInt32,
        records_inserted UInt32,
        duration_ms Float32,
        error_message String,
        started_at DateTime64(3, 'UTC'),
        completed_at DateTime64(3, 'UTC')
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(started_at)
    ORDER BY (status, started_at, sync_id)
    SETTINGS index_granularity = 8192;
    """,

    # 7. Timechamp Daily Department Summary (SummingMergeTree)
    f"""
    CREATE TABLE IF NOT EXISTS {settings.CLICKHOUSE_DB}.timechamp_daily_department_summary (
        timesheet_date Date,
        department_name LowCardinality(String),
        active_employees_count UInt32,
        total_working_seconds UInt64,
        total_productive_seconds UInt64,
        total_idle_seconds UInt64,
        total_unproductive_seconds UInt64,
        total_working_hours Float64 DEFAULT round(total_working_seconds / 3600.0, 2),
        total_productive_hours Float64 DEFAULT round(total_productive_seconds / 3600.0, 2),
        total_idle_hours Float64 DEFAULT round(total_idle_seconds / 3600.0, 2),
        total_unproductive_hours Float64 DEFAULT round(total_unproductive_seconds / 3600.0, 2)
    ) ENGINE = SummingMergeTree((active_employees_count, total_working_seconds, total_productive_seconds, total_idle_seconds, total_unproductive_seconds))
    PARTITION BY toYYYYMM(timesheet_date)
    PRIMARY KEY (timesheet_date, department_name)
    ORDER BY (timesheet_date, department_name)
    SETTINGS index_granularity = 8192;
    """
]


def run_migrations():
    """Execute initial schema setup against ClickHouse."""
    logger.info(f"Connecting to ClickHouse ({settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}) for migrations...")
    try:
        # Initial connection without specific DB to ensure creation
        client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            secure=settings.CLICKHOUSE_SECURE,
            connect_timeout=10,
        )

        for ddl in MIGRATION_DDLS:
            logger.info("Executing DDL statement...")
            client.command(ddl)

        logger.info("✓ All ClickHouse schema migrations executed successfully.")
        client.close()
    except Exception as e:
        logger.error(f"Failed to execute ClickHouse migrations: {e}")
        raise e


if __name__ == "__main__":
    run_migrations()
