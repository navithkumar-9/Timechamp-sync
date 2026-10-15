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
        id UUID DEFAULT generateUUIDv4(),
        emp_id LowCardinality(String) COMMENT 'Employee ID referencing COMMAND_CENTER.Employee_list(Employee_ID)',
        username String COMMENT 'Employee email/login username',
        name String COMMENT 'Employee full name',
        department_name LowCardinality(String) COMMENT 'Employee Department',
        reporting_head_name String COMMENT 'Reporting managers / supervisors',
        timesheet_date Date COMMENT 'Normalized timesheet date (YYYY-MM-DD)',
        timesheet_date_raw LowCardinality(String) COMMENT 'Original timesheet date string',
        start_date_time Nullable(DateTime) COMMENT 'Normalized clock-in timestamp',
        start_date_time_raw String COMMENT 'Original clock-in timestamp string',
        finish_date_time Nullable(DateTime) COMMENT 'Normalized clock-out timestamp',
        finish_date_time_raw String COMMENT 'Original clock-out timestamp string',
        total_time UInt32 COMMENT 'Total elapsed time in seconds',
        working_time UInt32 COMMENT 'Total active working time in seconds',
        productive_time UInt32 COMMENT 'Total productive time in seconds',
        idle_time UInt32 COMMENT 'Total idle time in seconds',
        unproductive_time UInt32 COMMENT 'Total unproductive time in seconds',
        neutral_time UInt32 COMMENT 'Total neutral time in seconds',
        idle_time_spent_on_productive_applications UInt32,
        idle_time_spent_on_unproductive_applications UInt32,
        idle_time_spent_on_neutral_applications UInt32,
        total_productive_time UInt32,
        total_unproductive_time UInt32,
        total_neutral_time UInt32,
        total_activity_count UInt32,
        productive_activity_count UInt32,
        unproductive_activity_count UInt32,
        neutral_activity_count UInt32,
        productive_percentage Float32,
        working_percentage Float32,
        fetched_at DateTime DEFAULT now(),
        updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
        total_hours Float32 DEFAULT round(total_time / 3600.0, 2),
        working_hours Float32 DEFAULT round(working_time / 3600.0, 2),
        productive_hours Float32 DEFAULT round(productive_time / 3600.0, 2),
        idle_hours Float32 DEFAULT round(idle_time / 3600.0, 2),
        unproductive_hours Float32 DEFAULT round(unproductive_time / 3600.0, 2),
        neutral_hours Float32 DEFAULT round(neutral_time / 3600.0, 2),
        idle_hours_spent_on_productive_applications Float32 DEFAULT round(idle_time_spent_on_productive_applications / 3600.0, 2),
        idle_hours_spent_on_unproductive_applications Float32 DEFAULT round(idle_time_spent_on_unproductive_applications / 3600.0, 2),
        idle_hours_spent_on_neutral_applications Float32 DEFAULT round(idle_time_spent_on_neutral_applications / 3600.0, 2),
        total_productive_hours Float32 DEFAULT round(total_productive_time / 3600.0, 2),
        total_unproductive_hours Float32 DEFAULT round(total_unproductive_time / 3600.0, 2),
        total_neutral_hours Float32 DEFAULT round(total_neutral_time / 3600.0, 2)
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(timesheet_date)
    PRIMARY KEY (department_name, timesheet_date, emp_id)
    ORDER BY (department_name, timesheet_date, emp_id)
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
