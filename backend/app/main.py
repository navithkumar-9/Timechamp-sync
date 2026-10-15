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
