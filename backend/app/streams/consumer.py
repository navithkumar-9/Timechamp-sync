import asyncio
import json
import os
from typing import Any, Optional
from app.core.config import settings
from app.core.logger import logger
from app.streams.handlers import handle_default_event, handle_timechamp_sync_event

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


class RedisStreamConsumer:
    """Async background worker consuming events from Redis Streams with consumer groups."""

    def __init__(self):
        self._redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
        self._stream_name = settings.REDIS_STREAM_EVENTS
        self._group_name = settings.REDIS_STREAM_GROUP
        self._consumer_name = f"consumer-{os.getpid()}"
        self._running = False
        self._client: Optional[Any] = None

    async def _setup_group(self, client: Any) -> None:
        """Create consumer group if it doesn't already exist."""
        try:
            await client.xgroup_create(
                name=self._stream_name,
                groupname=self._group_name,
                id="$",
                mkstream=True,
            )
            logger.info(f"✓ Created Redis Stream consumer group '{self._group_name}'.")
        except Exception as e:
            # Group already exists (BUSYGROUP)
            if "BUSYGROUP" not in str(e):
                logger.warning(f"Notice on stream group creation: {e}")

    async def start(self) -> None:
        if aioredis is None:
            logger.warning("Redis client library not installed; stream consumer inactive.")
            return

        self._running = True
        logger.info(f"Starting Redis Stream consumer on '{self._stream_name}' (group: '{self._group_name}')...")

        while self._running:
            try:
                self._client = aioredis.from_url(
                    self._redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                )
                await self._client.ping()
                await self._setup_group(self._client)
                logger.info(f"✓ Redis Stream consumer actively listening on '{self._stream_name}'...")
                break
            except Exception as e:
                logger.warning(f"Waiting for Redis connection: {e}. Retrying in 3 seconds...")
                await asyncio.sleep(3)

        while self._running and self._client:
            try:
                # Read new messages for this consumer group
                entries = await self._client.xreadgroup(
                    groupname=self._group_name,
                    consumername=self._consumer_name,
                    streams={self._stream_name: ">"},
                    count=10,
                    block=2000,
                )

                if not entries:
                    await asyncio.sleep(0.5)
                    continue

                for stream_name, messages in entries:
                    for msg_id, fields in messages:
                        event_type = fields.get("event_type", "unknown")
                        payload_raw = fields.get("payload", "{}")
                        try:
                            payload = json.loads(payload_raw)
                        except Exception:
                            payload = {"raw": payload_raw}

                        # Dispatch to appropriate handler
                        if event_type == "timechamp_sync":
                            await handle_timechamp_sync_event(payload)
                        else:
                            await handle_default_event(event_type, payload)

                        # Acknowledge message processed
                        await self._client.xack(self._stream_name, self._group_name, msg_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Redis Stream consumer loop: {e}")
                await asyncio.sleep(2)

        await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._client:
            try:
                await self._client.aclose()
            except Exception:
                pass
            finally:
                self._client = None
        logger.info("✓ Redis Stream consumer stopped cleanly.")


stream_consumer = RedisStreamConsumer()
