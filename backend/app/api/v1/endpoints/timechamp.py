from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Query, status
from app.db.clickhouse import ch_manager
from app.integrations.timechamp.client import get_timechamp_api_client
from app.integrations.timechamp.interfaces import (
    ITimeChampClient,
    ITrackingPipeline,
    ITrackingSummaryRepository,
)
from app.integrations.timechamp.models import SyncResult, TrackingSummaryFilter
from app.integrations.timechamp.pipeline import get_sync_pipeline
from app.integrations.timechamp.repository import get_repository
from app.schemas.common import StandardResponse
from app.schemas.timechamp import TimeChampFilter
from app.services.timechamp_service import TimeChampService, get_timechamp_service
from app.tasks.sample_tasks import sync_timechamp_tracking_data_task

router = APIRouter()


# ==============================================================================
# 1. CORE PIPELINE: API FETCH -> TRANSFORM -> CLICKHOUSE STORAGE
# ==============================================================================

@router.post(
    "/sync",
    response_model=StandardResponse[Dict[str, Any]],
    status_code=status.HTTP_200_OK,
    summary="Trigger Timechamp Data Fetch & ClickHouse Ingestion",
)
async def trigger_timechamp_sync(
    async_mode: bool = Query(False, description="Run in background via Celery worker if True"),
    target_date: Optional[str] = Query(None, description="Specific date to sync (e.g. '13-Oct-26' or '2026-10-13')"),
    lookback_days: int = Query(1, description="Number of past days to backfill for offline work"),
    auto_catchup: bool = Query(True, description="Automatically detect and backfill missing dates if server was down"),
    pipeline: ITrackingPipeline = Depends(get_sync_pipeline),
):
    """Fetches latest tracking summary data from Timechamp external API and saves it directly to ClickHouse.
    Runs synchronously by default or dispatches to Celery worker cluster if async_mode=True."""
    if async_mode:
        task = sync_timechamp_tracking_data_task.delay(sync_type="CELERY_ASYNC")
        return StandardResponse(
            success=True,
            message="Timechamp sync job queued onto Celery worker cluster",
            data={"task_id": task.id, "status": "QUEUED"},
        )

    result: SyncResult = await pipeline.run_sync(
        sync_type="MANUAL_API_TRIGGER",
        target_date=target_date,
        lookback_days=lookback_days,
        auto_catchup=auto_catchup,
    )

    return StandardResponse(
        success=result.success,
        message=result.message,
        data=result.model_dump(),
    )


@router.get(
    "/tracking-summary",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Query Stored Tracking Summary Records",
)
async def get_tracking_summary(
    emp_id: Optional[str] = Query(None, description="Filter by Employee ID"),
    department_name: Optional[str] = Query(None, description="Filter by Department"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    repo: ITrackingSummaryRepository = Depends(get_repository),
):
    """Queries normalized tracking summary records from ClickHouse COMMAND_CENTER.TimeChamp."""
    filters = TrackingSummaryFilter(
        emp_id=emp_id,
        department_name=department_name,
        limit=limit,
        offset=offset,
    )
    records = await repo.query_tracking_summary(filters)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(records)} tracking summary records",
        data=records,
    )


@router.get(
    "/sync-history",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Get Sync Audit & Execution History",
)
async def get_sync_history(
    limit: int = Query(20, ge=1, le=100),
):
    """Inspect execution history logs and audit metrics for API extractions."""
    query = f"""
        SELECT
            sync_id,
            sync_type,
            status,
            records_fetched,
            records_inserted,
            duration_ms,
            error_message,
            started_at,
            completed_at
        FROM {ch_manager._client.database if ch_manager._client else 'COMMAND_CENTER'}.timechamp_sync_history
        ORDER BY started_at DESC
        LIMIT {limit}
    """
    records = await ch_manager.query(query)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(records)} sync audit logs",
        data=records,
    )


@router.get(
    "/live-preview",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Live API Preview (Without Storage)",
)
async def live_api_preview(
    client: ITimeChampClient = Depends(get_timechamp_api_client),
):
    """Directly calls Timechamp external API using cached/authenticated token and returns live records."""
    records = await client.get_tracking_summary_data()
    return StandardResponse(
        success=True,
        message=f"Fetched {len(records)} live records from Timechamp API",
        data=records[:10], # Return first 10 for preview
    )


# ==============================================================================
# 2. LEGACY / EXISTING CLICKHOUSE TABLE QUERIES
# ==============================================================================

@router.post(
    "/records",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Query Raw TimeChamp Table",
)
async def query_timechamp_records(
    filter_params: TimeChampFilter,
    service: TimeChampService = Depends(get_timechamp_service),
):
    """Query legacy employee attendance & productivity records from COMMAND_CENTER.TimeChamp."""
    records = await service.get_timechamp_records(filter_params)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(records)} records from COMMAND_CENTER.TimeChamp",
        data=records,
    )


@router.get(
    "/employees",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="List Employee Directory",
)
async def list_employees(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: TimeChampService = Depends(get_timechamp_service),
):
    """Retrieve employee directory from COMMAND_CENTER.Employee_list."""
    records = await service.get_employees(limit=limit, offset=offset)
    return StandardResponse(
        success=True,
        message=f"Retrieved {len(records)} employees from ClickHouse",
        data=records,
    )


@router.get(
    "/department-summary",
    response_model=StandardResponse[List[Dict[str, Any]]],
    summary="Department Attendance Distribution",
)
async def get_department_summary(
    service: TimeChampService = Depends(get_timechamp_service),
):
    """Aggregate attendance status distribution by department."""
    summary = await service.get_department_attendance_summary()
    return StandardResponse(
        success=True,
        message="Department attendance aggregates generated",
        data=summary,
    )
