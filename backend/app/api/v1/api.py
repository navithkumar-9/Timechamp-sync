from fastapi import APIRouter
from app.api.v1.endpoints import health, metrics, timechamp

api_router = APIRouter()

api_router.include_router(health.router, tags=["Health & Probes"])
api_router.include_router(metrics.router, prefix="/metrics", tags=["Metrics & Ingestion"])
api_router.include_router(timechamp.router, prefix="/timechamp", tags=["Timechamp Production Analytics"])
