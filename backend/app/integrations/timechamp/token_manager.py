import asyncio
from typing import Any, Optional


try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None

from app.core.config import settings
from app.core.logger import logger
from app.integrations.timechamp.interfaces import ITokenManager



class RedisTokenManager(ITokenManager):
    """Token manager implementing token caching with Redis and automatic in-memory fallback."""

    def __init__(self, redis_host: str = settings.REDIS_HOST, redis_port: int = settings.REDIS_PORT, db: int = settings.REDIS_DB):
        self._redis_url = f"redis://{redis_host}:{redis_port}/{db}"
        self._cache_key = "timechamp:v5:auth_token"
        self._lock = asyncio.Lock()
        self._memory_token: Optional[str] = None

    async def _get_redis_conn(self) -> Optional[Any]:
        if aioredis is None:
            return None
        try:
            client = aioredis.from_url(self._redis_url, encoding="utf-8", decode_responses=True)
            await client.ping()
            return client
        except Exception:
            return None


    async def get_token(self) -> Optional[str]:
        # 1. Try Redis
        try:
            r = await self._get_redis_conn()
            if r:
                token = await r.get(self._cache_key)
                await r.aclose()
                if token:
                    return token
        except Exception as e:
            logger.debug(f"Redis get_token fallback: {e}")

        # 2. In-memory fallback
        return self._memory_token

    async def set_token(self, token: str, ttl_seconds: int = 43200) -> None:
        self._memory_token = token
        try:
            r = await self._get_redis_conn()
            if r:
                await r.set(self._cache_key, token, ex=ttl_seconds)
                await r.aclose()
                logger.debug("✓ Timechamp auth token cached in Redis.")
        except Exception as e:
            logger.debug(f"Could not cache token in Redis, using in-memory store: {e}")

    async def invalidate_token(self) -> None:
        self._memory_token = None
        try:
            r = await self._get_redis_conn()
            if r:
                await r.delete(self._cache_key)
                await r.aclose()
                logger.info("Timechamp auth token invalidated from cache.")
        except Exception as e:
            logger.debug(f"Error purging token from Redis: {e}")


default_token_manager = RedisTokenManager()


def get_token_manager() -> ITokenManager:
    return default_token_manager
