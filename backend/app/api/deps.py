from typing import AsyncGenerator
import redis.asyncio as aioredis
from app.core.config import settings
from app.db.clickhouse import ClickHouseManager, get_clickhouse_manager
from app.streams.producer import RedisStreamProducer, get_stream_producer
from app.services.metrics_service import MetricsService, get_metrics_service


async def get_redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    """Async Redis connection dependency."""
    client = aioredis.from_url(
        f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}",
        encoding="utf-8",
        decode_responses=True,
    )
    try:
        yield client
    finally:
        await client.aclose()


# Aliases for FastAPI Depends injection
ClickHouseDep = get_clickhouse_manager
StreamProducerDep = get_stream_producer
MetricsServiceDep = get_metrics_service

