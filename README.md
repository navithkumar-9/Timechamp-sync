# Timechamp Production Architecture & ETL Pipeline

A high-performance, lightweight asynchronous analytics backend and ETL engine built with **FastAPI**, **ClickHouse**, **Redis (Celery & Event Streams)**.

---

## 🏗 End-to-End Extraction & Ingestion Pipeline

```
┌─────────────────────────────────┐
│   Timechamp v5 Cloud API        │
│   • Authorise (POST)            │
│   • GetTrackingSummaryData (GET)│
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   ITimeChampClient              │ (Async HTTP with auto-token refresh & 401 recovery)
│   + ITokenManager               │ (Redis token caching with TTL)
└────────────────┬────────────────┘
                 │ Raw JSON
                 ▼
┌─────────────────────────────────┐
│   ITrackingDataTransformer      │ (Date normalization, percentage math, safe casting)
└────────────────┬────────────────┘
                 │ Domain Models
                 ▼
┌─────────────────────────────────┐
│   ITrackingSummaryRepository    │
└────────┬────────────────────────┘
         │
         ├───► High-Speed Native Batch Insert
         │     ▼
         │   ┌────────────────────────────────────────────────────────┐
         │   │ ClickHouse Database: COMMAND_CENTER                    │
         │   │ • timechamp_tracking_summary (ReplacingMergeTree)      │
         │   │ • timechamp_sync_history (MergeTree audit logs)        │
         │   │ • timechamp_daily_department_summary (SummingMergeTree)│
         │   └────────────────────────────────────────────────────────┘
         │
         └───► Lightweight Event Stream (XADD / XREADGROUP)
               ▼
             ┌────────────────────────────────────────────────────────┐
             │ Redis 7 Streams (timechamp:events:stream)              │
             │ Zero extra containers, <15MB RAM, instant startup      │
             └────────────────────────────────────────────────────────┘
```

---

## 💡 Why Redis Streams Instead of Kafka?

For small-to-medium throughput datasets (thousands to hundreds of thousands of events/day instead of billions):

1. **Zero Extra Containers**: Redis 7 is already running in our stack for the Celery broker, Celery backend, and token cache. Redis Streams is built into Redis, requiring no additional infrastructure.
2. **Resource Savings**: Dropping Apache Kafka and ZooKeeper/KRaft saves **500MB–1.5GB of RAM** and completely eliminates JVM CPU overhead.
3. **Kafka-Like Semantics**: Redis Streams (`XADD`, `XREADGROUP`, `XACK`) provides message persistence, consumer groups, offsets, and at-least-once delivery.
4. **Sub-millisecond Latency**: Eliminates broker network roundtrips and complex cluster coordination.

---

## 🏛 SOLID Principles Implementation

The architecture strictly follows the five **SOLID** principles:

| Principle | Implementation Details |
|---|---|
| **Single Responsibility (SRP)** | - `RedisTokenManager`: Token lifecycle and Redis TTL.<br>- `TimeChampHttpApiClient`: External HTTP calls with connection pooling.<br>- `TrackingDataTransformer`: Data normalization and calculation.<br>- `ClickHouseTrackingSummaryRepository`: ClickHouse persistence.<br>- `TrackingSyncPipeline`: End-to-end sync workflow.<br>- `RedisStreamProducer`: Event streaming. |
| **Open/Closed (OCP)** | Every layer depends on abstractions (`interfaces.py`). New endpoints or destination sinks can be introduced without altering pipeline orchestrators. |
| **Liskov Substitution (LSP)** | Implementations strictly fulfill their base contracts (`ITimeChampClient`, `ITrackingSummaryRepository`). |
| **Interface Segregation (ISP)** | Focused, lean interfaces: `ITokenManager`, `ITimeChampClient`, `ITrackingDataTransformer`, `ITrackingSummaryRepository`, and `ITrackingPipeline`. |
| **Dependency Inversion (DIP)** | Pipeline orchestrators and API controllers receive interfaces via Dependency Injection (`Depends`). |

---

## 📊 ClickHouse Production Schema

### 1. Primary Tracking Summary (`COMMAND_CENTER.timechamp_tracking_summary`)
- **Engine**: `ReplacingMergeTree(updated_at)` (Guarantees zero duplicates and maintains latest intraday state)
- **Partition Key**: `PARTITION BY toYYYYMM(timesheet_date)`
- **Composite Primary Key**: `(department_name, timesheet_date, emp_id)`
- **Sorting Key**: `ORDER BY (department_name, timesheet_date, emp_id)`
- **Foreign Key**: `emp_id` references `COMMAND_CENTER.Employee_list(Employee_ID)`

```sql
CREATE TABLE IF NOT EXISTS COMMAND_CENTER.timechamp_tracking_summary (
    id UUID DEFAULT generateUUIDv4(),
    emp_id LowCardinality(String) COMMENT 'Employee ID referencing COMMAND_CENTER.Employee_list(Employee_ID)',
    username String COMMENT 'Employee corporate email/login',
    name String COMMENT 'Employee display name',
    department_name LowCardinality(String) COMMENT 'Department',
    reporting_head_name String COMMENT 'Supervisor names',
    timesheet_date Date COMMENT 'Normalized date (YYYY-MM-DD)',
    timesheet_date_raw LowCardinality(String) COMMENT 'Original date string from API',
    start_date_time Nullable(DateTime) COMMENT 'Normalized clock-in timestamp',
    start_date_time_raw String COMMENT 'Raw clock-in string',
    finish_date_time Nullable(DateTime) COMMENT 'Normalized clock-out timestamp',
    finish_date_time_raw String COMMENT 'Raw clock-out string',
    total_time UInt32 COMMENT 'Total elapsed seconds',
    working_time UInt32 COMMENT 'Active working seconds',
    productive_time UInt32 COMMENT 'Productive seconds',
    idle_time UInt32 COMMENT 'Idle seconds',
    unproductive_time UInt32 COMMENT 'Unproductive seconds',
    neutral_time UInt32 COMMENT 'Neutral seconds',
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
    productive_percentage Float32 COMMENT 'Productive % of total time',
    working_percentage Float32 COMMENT 'Working % of total time',
    fetched_at DateTime DEFAULT now(),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3),
    total_hours Float32 DEFAULT round(total_time / 3600.0, 2) COMMENT 'Total elapsed hours',
    working_hours Float32 DEFAULT round(working_time / 3600.0, 2) COMMENT 'Active working hours',
    productive_hours Float32 DEFAULT round(productive_time / 3600.0, 2) COMMENT 'Productive hours',
    idle_hours Float32 DEFAULT round(idle_time / 3600.0, 2) COMMENT 'Idle hours',
    unproductive_hours Float32 DEFAULT round(unproductive_time / 3600.0, 2) COMMENT 'Unproductive hours',
    neutral_hours Float32 DEFAULT round(neutral_time / 3600.0, 2) COMMENT 'Neutral hours',
    idle_hours_spent_on_productive_applications Float32 DEFAULT round(idle_time_spent_on_productive_applications / 3600.0, 2),
    idle_hours_spent_on_unproductive_applications Float32 DEFAULT round(idle_time_spent_on_unproductive_applications / 3600.0, 2),
    idle_hours_spent_on_neutral_applications Float32 DEFAULT round(idle_time_spent_on_neutral_applications / 3600.0, 2),
    total_productive_hours Float32 DEFAULT round(total_productive_time / 3600.0, 2),
    total_unproductive_hours Float32 DEFAULT round(total_unproductive_time / 3600.0, 2),
    total_neutral_hours Float32 DEFAULT round(total_neutral_time / 3600.0, 2)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(timesheet_date)
PRIMARY KEY (department_name, timesheet_date, emp_id)
ORDER BY (department_name, timesheet_date, emp_id);
```

### 2. Execution Audit & Health History (`COMMAND_CENTER.timechamp_sync_history`)
- **Engine**: `MergeTree()`
- **Partition Key**: `PARTITION BY toYYYYMM(started_at)`
- **Sorting Key**: `ORDER BY (status, started_at, sync_id)`

---

## 🚀 Quick Start & Management

### 1. Launch Services
```bash
make up
# or
docker compose up -d
```

### 2. Useful Commands

| Command | Action |
|---|---|
| `make up` | Start full multi-container stack in development mode |
| `make up-prod` | Start stack with production limits and JSON logging |
| `make down` | Gracefully stop and clean up containers |
| `make logs` | Stream logs for all containers |
| `make ch-shell` | Connect directly to ClickHouse SQL client |
| `make redis-streams` | View recent events on Redis Stream |
| `make redis-cli` | Open interactive Redis shell |
| `make test` | Run pytest suite |

### 3. Interactive API Endpoints

- **Swagger Documentation**: [http://localhost:8000/api/v1/docs](http://localhost:8000/api/v1/docs)

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/timechamp/sync` | POST | Trigger extraction from Timechamp API into ClickHouse (`lookback_days=1`, `async_mode=true` dispatches to Celery) |
| `/api/v1/timechamp/tracking-summary` | GET | Query stored tracking records with filtering (`emp_id`, `department_name`, `start_date`) |
| `/api/v1/timechamp/sync-history` | GET | Query audit execution logs from `timechamp_sync_history` |
| `/api/v1/timechamp/live-preview` | GET | Test API connectivity and preview live external records |
