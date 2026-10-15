import uuid
from datetime import datetime
from typing import Any, Dict, List
from app.core.config import settings
from app.core.logger import logger
from app.db.clickhouse import ClickHouseManager, ch_manager
from app.streams.producer import RedisStreamProducer, stream_producer
from app.schemas.events import EventType
from app.schemas.metrics import MetricAggregateResult, MetricCreate, MetricQueryFilter, MetricRead
from app.tasks.sample_tasks import generate_tenant_analytics_report


class MetricsService:
    def __init__(self, ch: ClickHouseManager = ch_manager, sp: RedisStreamProducer = stream_producer):
        self.ch = ch
        self.sp = sp

    async def ingest_metric(self, metric: MetricCreate, stream_event: bool = True) -> MetricRead:
        """Ingest metric. If stream_event is True, publishes event onto Redis Stream for async processing.
        Also persists directly to ClickHouse."""
        record_id = uuid.uuid4()
        ts = metric.timestamp or datetime.utcnow()

        record = MetricRead(
            id=record_id,
            tenant_id=metric.tenant_id,
            device_id=metric.device_id,
            metric_name=metric.metric_name,
            value=metric.value,
            unit=metric.unit,
            tags=metric.tags,
            timestamp=ts,
            created_at=datetime.utcnow(),
        )

        # Always direct insert for analytics
        await self._direct_insert(record)

        if stream_event:
            event_payload = record.model_dump(mode="json")
            try:
                await self.sp.publish_event(
                    event_type="metric.ingested",
                    payload=event_payload,
                )
            except Exception as e:
                logger.debug(f"Stream publication notice: {e}")

        return record


    async def _direct_insert(self, record: MetricRead) -> None:
        data = [[
            str(record.id),
            record.tenant_id,
            record.device_id,
            record.metric_name,
            float(record.value),
            record.unit,
            record.tags,
            record.timestamp,
        ]]
        columns = ["id", "tenant_id", "device_id", "metric_name", "value", "unit", "tags", "timestamp"]
        await self.ch.insert(table="metrics", data=data, column_names=columns)

    async def query_metrics(self, q_filter: MetricQueryFilter) -> List[Dict[str, Any]]:
        """Query ClickHouse for analytical metric logs based on criteria."""
        conditions = [f"tenant_id = '{q_filter.tenant_id}'"]

        if q_filter.metric_name:
            conditions.append(f"metric_name = '{q_filter.metric_name}'")
        if q_filter.device_id:
            conditions.append(f"device_id = '{q_filter.device_id}'")
        if q_filter.start_time:
            conditions.append(f"timestamp >= '{q_filter.start_time.strftime('%Y-%m-%d %H:%M:%S')}'")
        if q_filter.end_time:
            conditions.append(f"timestamp <= '{q_filter.end_time.strftime('%Y-%m-%d %H:%M:%S')}'")

        where_clause = " AND ".join(conditions)
        query = f"""
            SELECT id, tenant_id, device_id, metric_name, value, unit, tags, timestamp, created_at
            FROM {settings.CLICKHOUSE_DB}.metrics
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT {q_filter.limit} OFFSET {q_filter.offset}
        """

        return await self.ch.query(query)

    async def get_tenant_aggregates(self, tenant_id: str) -> List[MetricAggregateResult]:
        """Compute real-time summary statistics grouped by metric name."""
        query = f"""
            SELECT
                tenant_id,
                metric_name,
                count() as count,
                avg(value) as avg_value,
                min(value) as min_value,
                max(value) as max_value
            FROM {settings.CLICKHOUSE_DB}.metrics
            WHERE tenant_id = '{tenant_id}'
            GROUP BY tenant_id, metric_name
        """
        rows = await self.ch.query(query)
        return [MetricAggregateResult(**row) for row in rows]

    def trigger_celery_report(self, tenant_id: str, report_type: str = "daily") -> str:
        """Enqueue long-running reporting computation onto Celery worker."""
        task = generate_tenant_analytics_report.delay(tenant_id, report_type)
        return task.id


metrics_service = MetricsService()


def get_metrics_service() -> MetricsService:
    return metrics_service
