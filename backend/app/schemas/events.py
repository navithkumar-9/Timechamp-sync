from datetime import datetime
from enum import Enum
from typing import Any, Dict
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class EventType(str, Enum):
    METRIC_INGESTED = "metric.ingested"
    ALERT_TRIGGERED = "alert.triggered"
    SYSTEM_MAINTENANCE = "system.maintenance"
    USER_ACTION = "user.action"


class KafkaEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    source: str = Field(default="api.service")
    payload: Dict[str, Any]
    status: str = Field(default="PENDING")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
