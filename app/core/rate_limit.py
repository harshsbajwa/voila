"""
Centralized rate limiting configuration and handlers
"""

import logging
from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from datetime import datetime

from shared.config import settings

logger = logging.getLogger(__name__)

# Create a single centralized limiter instance
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.api_rate_limit_per_minute}/minute"],
)


def rate_limit_exceeded_handler(
    request: Request, exc: RateLimitExceeded
) -> JSONResponse:
    """
    Standardized rate limit exceeded handler with consistent JSON response
    """
    # Extract rate limit info from the exception
    limit_string = (
        str(exc.detail) if hasattr(exc, "detail") else "Rate limit exceeded"
    )

    # Try to parse the limit from the string (e.g., "100 per 1 minute")
    try:
        parts = limit_string.split()
        if len(parts) >= 2:
            rate_limit = parts[0]
        else:
            rate_limit = str(settings.api_rate_limit_per_minute)
    except Exception:
        rate_limit = str(settings.api_rate_limit_per_minute)

    # Log rate limit hit
    client_host = request.client.host if request.client else "unknown"
    logger.warning(
        f"Rate limit exceeded for {client_host} on {request.url.path}"
    )

    response = JSONResponse(
        status_code=429,
        content={
            "error": True,
            "message": "Rate limit exceeded",
            "detail": "Too many requests. Please try again later.",
            "timestamp": datetime.now().isoformat(),
            "path": request.url.path,
            "status_code": 429,
        },
    )

    # Add standard rate limit headers
    response.headers["X-RateLimit-Limit"] = rate_limit
    response.headers["X-RateLimit-Remaining"] = "0"
    response.headers["Retry-After"] = "60"  # Suggest retry after 60 seconds

    return response


# Export decorator for convenience
limit = limiter.limit
