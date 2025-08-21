"""
Authentication and authorization utilities
"""

import logging
from typing import Optional
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from shared.config import settings

logger = logging.getLogger(__name__)

# API Key header scheme
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    api_key: Optional[str] = Security(api_key_header),
) -> str:
    """
    Verify API key for admin endpoints
    """
    # Get the expected API key from settings
    expected_key = getattr(settings, "admin_api_key", None)

    if not expected_key:
        # If no API key is configured, reject all admin requests in production
        if getattr(settings, "env", "development") == "production":
            logger.error("Admin API key not configured in production")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Admin endpoints not configured",
            )
        # Allow in development without key (but log warning)
        logger.warning(
            "Admin endpoint accessed without API key configuration (dev mode)"
        )
        return "development"

    if not api_key:
        logger.warning("Admin endpoint access attempted without API key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required for admin endpoints",
        )

    if api_key != expected_key:
        logger.warning(f"Invalid API key attempt: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key"
        )

    return api_key


# Optional: Role-based access control for future use
async def require_admin_role(api_key: str = Security(verify_api_key)) -> str:
    """
    Verify admin role (placeholder for future RBAC)
    """
    # For now, API key verification is sufficient
    # In future, could check roles/permissions here
    return api_key
