from typing import Any, Dict, List
from fastapi import APIRouter, Depends, Query, status
from app.api.deps import MetricsServiceDep
from app.schemas.common import StandardResponse
from app.schemas.metrics import (
    MetricAggregateResult,
    MetricBatchCreate,
    MetricCreate,
    MetricQueryFilter,
    MetricRead,
)
from app.services.metrics_service import MetricsService

router = APIRouter()


@router.post(
    "",
    response_model=StandardResponse[MetricRead],
    status_code=status.HTTP_201_CREATED,
    summary="Ingest Metric Event",
)
async def ingest_metric(
    metric: MetricCreate,
    stream_event: bool = Query(True, description="Publish event to Redis Stream for async processing"),
    service: MetricsService = Depends(MetricsServiceDep),
):
    """Ingests a telemetry/analytics metric. Streams to Redis Stream and persists to ClickHouse."""
    record = await service.ingest_metric(metric, stream_event=stream_event)
    return StandardResponse(
        success=True,
        message="Metric ingested and persisted to ClickHouse",
        data=record,
    )


@router.post(
    "/batch",
    response_model=StandardResponse[Dict[str, Any]],
    status_code=status.HTTP_201_CREATED,
    summary="Ingest Batch of Metrics",
)
async def ingest_metrics_batch(
    payload: MetricBatchCreate,
    stream_event: bool = Query(True, description="Publish events to Redis Stream"),
    service: MetricsService = Depends(MetricsServiceDep),
):
    """Ingests a batch of metrics in a single network request."""
    count = len(payload.metrics)
    for m in payload.metrics:
        await service.ingest_metric(m, stream_event=stream_event)

    return StandardResponse(
        success=True,
        message=f"Successfully processed batch of {count} metrics",
        data={"count": count, "streamed": stream_event},
    )



@router.post(
    "/query",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Query Analytical Metrics",
)
async def query_metrics(
    q_filter: MetricQueryFilter,
    service: MetricsService = Depends(MetricsServiceDep),
):
    """Executes high-performance analytical queries against ClickHouse."""
    records = await service.query_metrics(q_filter)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(records)} records from ClickHouse",
        data=records,
    )


@router.get(
    "/aggregates/{tenant_id}",
    response_model=StandardResponse[List[MetricAggregateResult]],
    summary="Get Tenant Summary Aggregates",
)
async def get_tenant_aggregates(
    tenant_id: str,
    service: MetricsService = Depends(MetricsServiceDep),
):
    """Fetches real-time statistical aggregates (count, average, min, max) from ClickHouse."""
    aggregates = await service.get_tenant_aggregates(tenant_id)
    return StandardResponse(
        success=True,
        message=f"Aggregates computed for tenant '{tenant_id}'",
        data=aggregates,
    )


@router.post(
    "/reports/generate",
    response_model=StandardResponse[Dict[str, str]],
    summary="Dispatch Celery Analytics Report Job",
)
async def trigger_analytics_report(
    tenant_id: str = Query(..., example="tenant-001"),
    report_type: str = Query("daily", example="daily"),
    service: MetricsService = Depends(MetricsServiceDep),
):
    """Dispatches a heavy asynchronous computation to the Celery worker cluster."""
    task_id = service.trigger_celery_report(tenant_id=tenant_id, report_type=report_type)
    return StandardResponse(
        success=True,
        message="Asynchronous report job dispatched to Celery",
        data={"task_id": task_id, "status": "QUEUED"},
    )
