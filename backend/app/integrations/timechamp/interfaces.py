from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict, List, Optional

from app.integrations.timechamp.models import (
    SyncHistoryRecord,
    SyncResult,
    TrackingSummaryFilter,
    TrackingSummaryRecord,
)


class ITokenManager(ABC):
    """Interface for managing authentication token lifecycle (SRP: Token Management)."""

    @abstractmethod
    async def get_token(self) -> str:
        """Retrieve a valid authentication token, refreshing if necessary."""
        pass

    @abstractmethod
    async def set_token(self, token: str, ttl_seconds: int = 43200) -> None:
        """Cache the authentication token."""
        pass

    @abstractmethod
    async def invalidate_token(self) -> None:
        """Purge any cached token to force a re-login on next attempt."""
        pass


class ITimeChampClient(ABC):
    """Interface for external Timechamp API communication (SRP: HTTP Network Operations)."""

    @abstractmethod
    async def authenticate(self) -> str:
        """Call Authorise API and return bearer auth token."""
        pass

    @abstractmethod
    async def get_tracking_summary_data(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Call GetTrackingSummaryData (optionally for a specific date) and return list of raw record dictionaries."""
        pass



class ITrackingDataTransformer(ABC):
    """Interface for transforming and sanitizing raw API data (SRP: Data Normalization)."""

    @abstractmethod
    def transform_record(self, raw: Dict[str, Any]) -> TrackingSummaryRecord:
        """Transform a single raw dictionary into a typed domain record."""
        pass

    @abstractmethod
    def transform_batch(self, raw_list: List[Dict[str, Any]]) -> List[TrackingSummaryRecord]:
        """Transform a collection of raw records."""
        pass


class ITrackingSummaryRepository(ABC):
    """Interface for database persistence (SRP: ClickHouse Storage & Queries)."""

    @abstractmethod
    async def save_batch(self, records: List[TrackingSummaryRecord]) -> int:
        """Batch insert records into ClickHouse."""
        pass

    @abstractmethod
    async def save_sync_history(self, log: SyncHistoryRecord) -> None:
        """Record an execution log entry into ClickHouse sync history table."""
        pass

    @abstractmethod
    async def query_tracking_summary(self, filters: TrackingSummaryFilter) -> List[Dict[str, Any]]:
        """Execute analytical query against ClickHouse."""
        pass

    @abstractmethod
    async def get_latest_synced_date(self) -> Optional[date]:
        """Fetch the most recent timesheet_date stored in ClickHouse."""
        pass

    @abstractmethod
    async def get_missing_dates(
        self, start_date: date, end_date: date, min_records: int = 50
    ) -> List[date]:
        """Identify any dates within range having fewer than min_records in ClickHouse."""
        pass

    @abstractmethod
    async def ensure_historical_data_restored(self) -> int:
        """Ensure historical records prior to active payroll cycle are restored from backup if missing."""
        pass


class ITrackingPipeline(ABC):
    """Interface for coordinating the end-to-end sync workflow (SRP: Orchestration)."""

    @abstractmethod
    async def run_sync(
        self,
        sync_type: str = "MANUAL",
        target_date: Optional[str] = None,
        lookback_days: int = 1,
        auto_catchup: bool = False,
        payroll_cycle: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> SyncResult:
        """Execute end-to-end extraction, transformation, and storage with optional lookback window, auto-catchup, or payroll cycle."""
        pass

