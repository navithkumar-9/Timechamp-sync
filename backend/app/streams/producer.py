import json
from typing import Any, Dict, Optional
from app.core.config import settings
from app.core.logger import logger

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


class RedisStreamProducer:
    """Lightweight event streaming producer using Redis Streams (XADD). Replaces heavy Kafka broker."""

    def __init__(self):
        self._redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
        self._client: Optional[Any] = None
        self._stream_name = settings.REDIS_STREAM_EVENTS

    async def _get_client(self) -> Optional[Any]:
        if aioredis is None:
            return None
        if self._client is None:
            try:
                self._client = aioredis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                )
                await self._client.ping()
            except Exception as e:
                logger.debug(f"Redis stream client not available: {e}")
                self._client = None
        return self._client

    async def is_healthy(self) -> bool:
        client = await self._get_client()
        if client:
            try:
                return await client.ping()
            except Exception:
                return False
        return False

    async def publish_event(self, event_type: str, payload: Dict[str, Any], stream_name: Optional[str] = None) -> Optional[str]:
        """Publish event dictionary onto a Redis stream using XADD."""
        target_stream = stream_name or self._stream_name
        client = await self._get_client()
        if not client:
            logger.debug(f"Event streaming skipped (Redis offline): {event_type}")
            return None

        try:
            fields = {
                "event_type": event_type,
                "payload": json.dumps(payload, default=str),
            }
            msg_id = await client.xadd(name=target_stream, fields=fields)
            logger.debug(f"Event published to Redis stream '{target_stream}' [msg_id: {msg_id}]")
            return msg_id
        except Exception as e:
            logger.warning(f"Failed to publish event to Redis stream '{target_stream}': {e}")
            return None

    async def close(self) -> None:
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            finally:
                self._client = None


stream_producer = RedisStreamProducer()


def get_stream_producer() -> RedisStreamProducer:
    return stream_producer
