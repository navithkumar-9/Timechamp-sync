import time
from fastapi import APIRouter, Response, status
import redis.asyncio as aioredis
from app.core.config import settings
from app.db.clickhouse import ch_manager
from app.streams.producer import stream_producer
from app.schemas.common import HealthResponse, ServiceHealth


router = APIRouter()


@router.get("/health/live", summary="Liveness Probe")
async def liveness_probe():
    """Returns 200 if the FastAPI application process is alive."""
    return {"status": "alive"}


@router.get("/health/ready", response_model=HealthResponse, summary="Readiness Probe")
async def readiness_probe(response: Response):
    """Verifies operational readiness of upstream dependencies (ClickHouse, Redis, Kafka)."""
    services = {}
    is_ready = True

    # 1. ClickHouse Check
    t0 = time.perf_counter()
    ch_alive = await ch_manager.ping()
    ch_latency = round((time.perf_counter() - t0) * 1000, 2)
    services["clickhouse"] = ServiceHealth(
        status="healthy" if ch_alive else "unreachable",
        latency_ms=ch_latency if ch_alive else None,
        details={"host": settings.CLICKHOUSE_HOST, "port": settings.CLICKHOUSE_PORT},
    )
    if not ch_alive:
        is_ready = False

    # 2. Redis Check
    t0 = time.perf_counter()
    redis_alive = False
    try:
        r = aioredis.from_url(f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}")
        pong = await r.ping()
        await r.aclose()
        redis_alive = bool(pong)
    except Exception:
        redis_alive = False
    redis_latency = round((time.perf_counter() - t0) * 1000, 2)
    services["redis"] = ServiceHealth(
        status="healthy" if redis_alive else "unreachable",
        latency_ms=redis_latency if redis_alive else None,
        details={"host": settings.REDIS_HOST, "port": settings.REDIS_PORT},
    )
    if not redis_alive:
        is_ready = False

    # 3. Redis Stream Check
    stream_alive = await stream_producer.is_healthy()
    services["redis_stream"] = ServiceHealth(
        status="healthy" if stream_alive else "degraded",
        details={"stream_events": settings.REDIS_STREAM_EVENTS},
    )


    overall_status = "ready" if is_ready else "not_ready"
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status=overall_status,
        environment=settings.ENVIRONMENT,
        services=services,
    )
