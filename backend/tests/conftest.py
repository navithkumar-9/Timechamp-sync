import os
import sys
from unittest.mock import AsyncMock, MagicMock
import pytest
from httpx import ASGITransport, AsyncClient

# Inject backend path into sys.path
backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if backend_path not in sys.path:
    sys.path.insert(0, backend_path)

from app.main import app
from app.db.clickhouse import ch_manager
from app.streams.producer import stream_producer


@pytest.fixture(autouse=True)
def mock_external_services(monkeypatch):
    """Automatically mock network calls to ClickHouse, Redis Streams, and Celery during test runs."""
    monkeypatch.setattr(ch_manager, "ping", AsyncMock(return_value=True))
    monkeypatch.setattr(ch_manager, "query", AsyncMock(return_value=[
        {
            "id": "e4eaaaf2-d142-11e1-b3e4-080027620cdd",
            "tenant_id": "test-tenant",
            "device_id": "dev-01",
            "metric_name": "cpu_load",
            "value": 45.2,
            "unit": "percent",
            "tags": {"env": "test"},
            "timestamp": "2026-10-14 12:00:00",
            "created_at": "2026-10-14 12:00:00",
        }
    ]))
    monkeypatch.setattr(ch_manager, "insert", AsyncMock(return_value=None))
    monkeypatch.setattr(stream_producer, "publish_event", AsyncMock(return_value="1609459200000-0"))
    monkeypatch.setattr(stream_producer, "is_healthy", AsyncMock(return_value=True))



from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Synchronous test client."""
    with TestClient(app) as c:
        yield c
