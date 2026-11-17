import asyncio
from typing import Any, Dict, List, Optional
import httpx
from app.core.config import settings
from app.core.exceptions import TimechampException
from app.core.logger import logger
from app.integrations.timechamp.interfaces import ITimeChampClient, ITokenManager
from app.integrations.timechamp.token_manager import get_token_manager


class TimeChampHttpApiClient(ITimeChampClient):
    """High-performance async HTTP client for Timechamp API with automated token lifecycle management."""

    def __init__(self, token_manager: ITokenManager = None):
        self.base_url = settings.TIMECHAMP_API_BASE_URL.rstrip("/")
        self.username = settings.TIMECHAMP_USERNAME
        self.password = settings.TIMECHAMP_PASSWORD
        self.token_manager = token_manager or get_token_manager()

        # Connection pooling and persistent timeouts
        self._limits = httpx.Limits(max_keepalive_connections=20, max_connections=50)
        self._timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=10.0)

    async def authenticate(self) -> str:
        """Call Authorise API and return bearer auth token."""
        url = f"{self.base_url}/backend/api/LoginApi/Authorise"
        payload = {
            "UserName": self.username,
            "Password": self.password,
        }

        logger.info(f"Authenticating with Timechamp API at {url} (user: {self.username})...")

        async with httpx.AsyncClient(limits=self._limits, timeout=self._timeout) as client:
            for attempt in range(1, 4):
                try:
                    response = await client.post(url, json=payload)
                    response.raise_for_status()
                    data = response.json()

                    if not data.get("success"):
                        messages = data.get("apiResponseMessages") or "Unknown error"
                        raise TimechampException(f"Timechamp login failed: {messages}", status_code=401)

                    auth_data = data.get("data") or {}
                    token = auth_data.get("authToken")
                    if not token:
                        raise TimechampException("Auth token missing from Timechamp login response")

                    # Cache the token
                    await self.token_manager.set_token(token)
                    logger.info("✓ Timechamp API authentication successful.")
                    return token

                except httpx.HTTPError as exc:
                    logger.warning(f"Authentication attempt {attempt} failed: {exc}")
                    if attempt == 3:
                        raise TimechampException(f"Failed to authenticate with Timechamp after 3 attempts: {str(exc)}")
                    await asyncio.sleep(2 ** attempt)

    async def get_tracking_summary_data(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Call GetTrackingSummaryData with automated token resolution and transparent 401/403 retry."""
        token = await self.token_manager.get_token()
        if not token:
            token = await self.authenticate()

        url = f"{self.base_url}/backend/ActivityTracker/ActivityTrackerApi/GetTrackingSummaryData"
        if target_date:
            url += f"?date={target_date}"

        async with httpx.AsyncClient(limits=self._limits, timeout=self._timeout) as client:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                try:
                    response = await client.get(url, headers=headers)

                    # Handle 401 Unauthorized or 403 Forbidden transparently by refreshing token
                    if response.status_code in (401, 403):
                        if attempt < max_attempts:
                            logger.warning(
                                f"Received HTTP {response.status_code} from Timechamp API (attempt {attempt}/{max_attempts}). "
                                f"Invalidating cached token, re-authenticating and retrying..."
                            )
                            await self.token_manager.invalidate_token()
                            token = await self.authenticate()
                            headers["Authorization"] = f"Bearer {token}"
                            await asyncio.sleep(1)
                            continue
                        else:
                            await self.token_manager.invalidate_token()
                            raise TimechampException(
                                f"Timechamp API returned HTTP {response.status_code} after {attempt} attempts: {response.text[:200]}"
                            )

                    response.raise_for_status()
                    result = response.json()

                    if not result.get("success"):
                        messages = result.get("apiResponseMessages") or "API reported failure"
                        msg_str = str(messages).lower()
                        # Handle potential auth or session expiration reported inside 200 OK body
                        if any(kw in msg_str for kw in ("unauthorized", "token", "forbidden", "session", "expired", "denied")):
                            if attempt < max_attempts:
                                logger.warning(
                                    f"Timechamp API reported auth failure in response body: {messages}. "
                                    f"Invalidating token, re-authenticating and retrying..."
                                )
                                await self.token_manager.invalidate_token()
                                token = await self.authenticate()
                                headers["Authorization"] = f"Bearer {token}"
                                await asyncio.sleep(1)
                                continue
                            else:
                                await self.token_manager.invalidate_token()
                        raise TimechampException(f"GetTrackingSummaryData returned failure: {messages}")

                    records = result.get("data")
                    if isinstance(records, list):
                        logger.info(f"✓ Retrieved {len(records)} tracking summary records from Timechamp API.")
                        return records
                    elif isinstance(records, dict):
                        return [records]
                    return []

                except httpx.HTTPError as exc:
                    logger.error(f"HTTP error fetching tracking summary (attempt {attempt}/{max_attempts}): {exc}")
                    if attempt == max_attempts:
                        raise TimechampException(f"Failed to fetch tracking summary: {str(exc)}")
                    await asyncio.sleep(2)


default_api_client = TimeChampHttpApiClient()


def get_timechamp_api_client() -> ITimeChampClient:
    return default_api_client
