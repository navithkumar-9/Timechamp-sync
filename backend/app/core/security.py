import secrets
from typing import Optional
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from app.core.config import settings

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


def generate_secure_token(nbytes: int = 32) -> str:
    """Generate a cryptographically secure random URL-safe token."""
    return secrets.token_urlsafe(nbytes)


async def verify_api_key(api_key: Optional[str] = Security(api_key_header)) -> Optional[str]:
    """Optional API key verification for secured production routes."""
    # In development mode, allow bypassing if not configured
    if settings.ENVIRONMENT == "development" and not api_key:
        return "dev-user"

    if api_key and secrets.compare_digest(api_key, settings.SECRET_KEY):
        return api_key

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid or missing API Key credentials",
    )
