import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ingest_single_metric(async_client: AsyncClient):
    payload = {
        "tenant_id": "tenant-xyz",
        "device_id": "macbook-pro-14",
        "metric_name": "active_window_duration_seconds",
        "value": 120.5,
        "unit": "seconds",
        "tags": {"app": "vscode"},
    }
    response = await async_client.post("/api/v1/metrics?stream_event=false", json=payload)

    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["data"]["tenant_id"] == "tenant-xyz"
    assert data["data"]["value"] == 120.5


@pytest.mark.asyncio
async def test_query_metrics(async_client: AsyncClient):
    payload = {
        "tenant_id": "test-tenant",
        "metric_name": "cpu_load",
        "limit": 10,
        "offset": 0,
    }
    response = await async_client.post("/api/v1/metrics/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 1
    assert data["data"][0]["metric_name"] == "cpu_load"
