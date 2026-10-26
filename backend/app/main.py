from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1.api import api_router
from app.core.config import settings
from app.core.exceptions import (
    TimechampException,
    global_exception_handler,
    timechamp_exception_handler,
)
from app.core.logger import logger
from app.db.clickhouse import ch_manager
from app.streams.producer import stream_producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Production lifespan context for initializing and tearing down asynchronous resources."""
    logger.info(f"Starting {settings.PROJECT_NAME} [{settings.ENVIRONMENT}]...")

    # 1. Initialize ClickHouse Connection Pool
    try:
        ch_manager.connect()
        logger.info("✓ ClickHouse client initialized.")
    except Exception as e:
        logger.warning(f"ClickHouse deferred initialization: {e}")

    # 2. Check Event Streaming (Redis Streams)
    if await stream_producer.is_healthy():
        logger.info("✓ Redis Stream Producer active.")

    # 3. Automatic Downtime Catch-up Sync on Startup with Distributed Lock
    async def _startup_catchup():
        await asyncio.sleep(2) # Brief delay to allow initial server startup to complete
        from app.integrations.timechamp.token_manager import get_token_manager
        tm = get_token_manager()

        # Connect to Redis with retry to ensure workers synchronize reliably
        r = None
        for _ in range(15):
            r = await tm._get_redis_conn()
            if r:
                break
            await asyncio.sleep(1)

        if not r:
            logger.warning("Could not establish Redis connection for startup lock. Skipping startup catchup.")
            return

        try:
            # Distributed lock ensures ONLY 1 worker process executes catchup on startup.
            # TTL=300s ensures other workers do not re-run catchup when worker 1 completes.
            acquired = await r.set("timechamp:lock:startup_catchup", "1", nx=True, ex=300)
            if not acquired:
                logger.info("Startup auto-catchup is already being handled by another worker process.")
                return

            from app.integrations.timechamp.pipeline import get_sync_pipeline
            pipe = get_sync_pipeline()
            logger.info("Checking for any missed dates during server downtime...")
            result = await pipe.run_sync(sync_type="STARTUP_AUTO_CATCHUP", auto_catchup=True)
            logger.info(f"✓ Startup auto-catchup finished: {result.records_inserted} rows updated.")
        except Exception as e:
            logger.warning(f"Startup catchup notice: {e}")
        finally:
            try:
                await r.aclose()
            except Exception:
                pass

    import asyncio
    asyncio.create_task(_startup_catchup())

    yield

    # Teardown
    logger.info("Initiating graceful shutdown of backend services...")
    await stream_producer.close()
    ch_manager.close()
    logger.info("✓ All services successfully stopped.")



def create_application() -> FastAPI:
    """Factory function for FastAPI application."""
    application = FastAPI(
        title=settings.PROJECT_NAME,
        version="1.0.0",
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
        docs_url=f"{settings.API_V1_STR}/docs",
        redoc_url=f"{settings.API_V1_STR}/redoc",
        lifespan=lifespan,
    )

    # CORS Middleware configuration
    application.add_middleware(
        CORSMiddleware,
        allow_origins=settings.ALLOWED_HOSTS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Exception Handlers
    application.add_exception_handler(TimechampException, timechamp_exception_handler)
    application.add_exception_handler(Exception, global_exception_handler)

    # Include API Routers
    application.include_router(api_router, prefix=settings.API_V1_STR)

    @application.get("/", tags=["Root"])
    async def root():
        return {
            "name": settings.PROJECT_NAME,
            "version": "1.0.0",
            "environment": settings.ENVIRONMENT,
            "docs": f"{settings.API_V1_STR}/docs",
        }

    return application


app = create_application()
