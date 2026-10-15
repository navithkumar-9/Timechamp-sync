from datetime import datetime
from typing import Dict, List, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class MetricBase(BaseModel):
    tenant_id: str = Field(..., example="tenant-001", min_length=1)
    device_id: str = Field(..., example="device-app-42", min_length=1)
    metric_name: str = Field(..., example="cpu_utilization_pct")
    value: float = Field(..., example=78.5)
    unit: str = Field(default="percent", example="percent")
    tags: Dict[str, str] = Field(default_factory=dict, example={"env": "prod", "region": "us-east-1"})
    timestamp: Optional[datetime] = Field(default_factory=datetime.utcnow)


class MetricCreate(MetricBase):
    pass


class MetricBatchCreate(BaseModel):
    metrics: List[MetricCreate] = Field(..., min_length=1)


class MetricRead(MetricBase):
    id: UUID = Field(default_factory=uuid4)
    created_at: Optional[datetime] = None


class MetricQueryFilter(BaseModel):
    tenant_id: str
    metric_name: Optional[str] = None
    device_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=10000)
    offset: int = Field(default=0, ge=0)


class MetricAggregateResult(BaseModel):
    tenant_id: str
    metric_name: str
    count: int
    avg_value: float
    min_value: float
    max_value: float
