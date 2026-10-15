import json
from typing import Any, Dict
from app.core.logger import logger


async def handle_timechamp_sync_event(payload: Dict[str, Any]) -> None:
    """Handle incoming sync completion events from the stream."""
    sync_id = payload.get("sync_id")
    records_inserted = payload.get("records_inserted", 0)
    logger.info(f"[Stream Consumer] Received sync notification: {sync_id} with {records_inserted} rows inserted.")


async def handle_default_event(event_type: str, payload: Dict[str, Any]) -> None:
    """Default handler for uncategorized stream events."""
    logger.debug(f"[Stream Consumer] Event processed: {event_type} | Data: {payload}")
