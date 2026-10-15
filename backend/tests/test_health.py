import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_root_endpoint(async_client: AsyncClient):
    response = await async_client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert data["environment"] == "development"


@pytest.mark.asyncio
async def test_liveness_probe(async_client: AsyncClient):
    response = await async_client.get("/api/v1/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


@pytest.mark.asyncio
async def test_readiness_probe(async_client: AsyncClient):
    response = await async_client.get("/api/v1/health/ready")
    assert response.status_code in (200, 503)
    data = response.json()
    assert "services" in data
    assert "clickhouse" in data["services"]
    assert "redis" in data["services"]
    assert "redis_stream" in data["services"]

