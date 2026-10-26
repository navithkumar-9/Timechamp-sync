from fastapi.testclient import TestClient


def test_ingest_single_metric(client: TestClient):
    payload = {
        "tenant_id": "tenant-xyz",
        "device_id": "macbook-pro-14",
        "metric_name": "active_window_duration_seconds",
        "value": 120.5,
        "unit": "seconds",
        "tags": {"app": "vscode"},
    }
    response = client.post("/api/v1/metrics?stream_event=false", json=payload)

    assert response.status_code == 201
    data = response.json()
    assert data["success"] is True
    assert data["data"]["tenant_id"] == "tenant-xyz"
    assert data["data"]["value"] == 120.5


def test_query_metrics(client: TestClient):
    payload = {
        "tenant_id": "test-tenant",
        "metric_name": "cpu_load",
        "limit": 10,
        "offset": 0,
    }
    response = client.post("/api/v1/metrics/query", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["data"], list)
    assert len(data["data"]) == 1
    assert data["data"][0]["metric_name"] == "cpu_load"
