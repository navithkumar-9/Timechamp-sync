from typing import Any, Dict, Optional
from app.core.logger import logger


class TimechampException(Exception):
    """Base exception for application domain errors."""

    def __init__(self, message: str, status_code: int = 500, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details or {}


class DatabaseError(TimechampException):
    def __init__(self, message: str = "Database operation failed", details: Optional[Dict[str, Any]] = None):
        super().__init__(message=message, status_code=503, details=details)


class KafkaPublishError(TimechampException):
    def __init__(self, message: str = "Failed to produce message to Kafka", details: Optional[Dict[str, Any]] = None):
        super().__init__(message=message, status_code=502, details=details)


class ResourceNotFoundError(TimechampException):
    def __init__(self, message: str = "Requested resource not found", details: Optional[Dict[str, Any]] = None):
        super().__init__(message=message, status_code=404, details=details)


async def timechamp_exception_handler(request, exc: TimechampException):
    from fastapi.responses import JSONResponse
    logger.error(f"Application error on {request.method} {request.url.path}: {exc.message} - Details: {exc.details}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": {
                "message": exc.message,
                "type": exc.__class__.__name__,
                "details": exc.details,
            },
        },
    )


async def global_exception_handler(request, exc: Exception):
    from fastapi.responses import JSONResponse
    logger.exception(f"Unhandled system error on {request.method} {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": {
                "message": "Internal Server Error",
                "type": "InternalServerError",
            },
        },
    )
