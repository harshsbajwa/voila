import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from app.core.database import questdb_manager, redis_manager
from app.core.async_database import async_questdb_manager, async_redis_manager
from app.core.cache_manager import cache_manager, cache_warming_job
from app.core.rate_limit import limiter, rate_limit_exceeded_handler
from app.core.auth import verify_api_key
from app.api.v1.endpoints import market_data, geospatial, core_data
from shared.config import settings

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Llifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown"""
    # Startup
    try:
        logger.info("Starting API...")

        # Initialize Async QuestDB connection pool
        await async_questdb_manager.initialize()
        logger.info("Async QuestDB connection pool initialized")

        # Initialize Async Redis connection
        await async_redis_manager.initialize()
        logger.info("Async Redis connection initialized")

        # Initialize cache manager
        await cache_manager.initialize()
        logger.info("Cache manager initialized")

        # Start background cache warming (don't await) and store task handle
        app.state.cache_warming_task = asyncio.create_task(cache_warming_job())
        logger.info("Cache warming job started")

        logger.info("All services initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise

    yield

    logger.info("Shutting down API...")

    # Cancel background tasks gracefully
    if hasattr(app.state, "cache_warming_task"):
        app.state.cache_warming_task.cancel()
        try:
            await app.state.cache_warming_task
        except asyncio.CancelledError:
            logger.info("Cache warming task cancelled")

    questdb_manager.close()
    redis_manager.close()
    await async_questdb_manager.close()
    await async_redis_manager.close()
    await cache_manager.close()
    logger.info("All connections closed")


app = FastAPI(
    title=settings.api_title,
    description=settings.api_description,
    version=settings.api_version,
    lifespan=lifespan,
)

# Rate limiting
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# Configure CORS based on environment
if settings.env == "production":
    cors_origins = [
        origin
        for origin in settings.cors_origins
        if origin != "*"
    ]
    if not cors_origins:
        cors_origins = ["http://localhost:8000"]
else:
    cors_origins = settings.cors_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.include_router(core_data.router, prefix="/api/v1")
app.include_router(market_data.router, prefix="/api/v1")
app.include_router(geospatial.router, prefix="/api/v1")


# Root endpoint
@app.get("/")
@limiter.limit("200/minute")
async def read_root(request: Request):
    return {
        "status": "ok",
        "version": settings.api_version,
        "endpoints": {
            "api_docs": "/docs",
            "core_data": "/api/v1/data/",
            "market_data": "/api/v1/market-data/",
            "geospatial": "/api/v1/spatial/",
            "health": "/health",
        },
    }


@app.get("/health")
@limiter.limit("1000/minute")
async def health_check(request: Request):
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {},
    }

    # Check QuestDB with timeout
    try:
        await asyncio.wait_for(
            async_questdb_manager.fetch_one(
                f"SELECT 1 as ok FROM {settings.questdb_ohlcv_table} LIMIT 1"
            ),
            timeout=2.0,
        )
        health_status["services"]["questdb"] = {
            "status": "healthy",
            "connection": "ok",
        }
    except asyncio.TimeoutError:
        health_status["services"]["questdb"] = {
            "status": "degraded",
            "error": "timeout",
        }
        health_status["status"] = "degraded"
    except Exception:
        health_status["services"]["questdb"] = {
            "status": "unhealthy",
            "error": "connection failed",
        }
        health_status["status"] = "degraded"

    # Check Redis with timeout
    try:
        if async_redis_manager.client:
            await asyncio.wait_for(
                async_redis_manager.client.ping(), timeout=2.0
            )
            health_status["services"]["redis"] = {
                "status": "healthy",
                "connection": "ok",
            }
        else:
            raise Exception("Redis client not initialized")
    except asyncio.TimeoutError:
        health_status["services"]["redis"] = {
            "status": "degraded",
            "error": "timeout",
        }
        health_status["status"] = "degraded"
    except Exception:
        health_status["services"]["redis"] = {
            "status": "unhealthy",
            "error": "connection failed",
        }
        health_status["status"] = "degraded"

    # Check Cache Manager with timeout
    try:
        await asyncio.wait_for(cache_manager.stats(), timeout=2.0)
        health_status["services"]["cache"] = {
            "status": "healthy",
            "stats": "available",
        }
    except asyncio.TimeoutError:
        health_status["services"]["cache"] = {
            "status": "degraded",
            "error": "timeout",
        }
        health_status["status"] = "degraded"
    except Exception:
        health_status["services"]["cache"] = {
            "status": "unhealthy",
            "error": "unavailable",
        }
        health_status["status"] = "degraded"

    return health_status


@app.get("/cache/stats")
@limiter.limit("60/minute")
async def cache_stats(request: Request):
    """Get cache statistics"""
    try:
        stats = await cache_manager.stats()
        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "cache_stats": stats,
        }
    except Exception as e:
        logger.error(f"Cache stats error: {e}")
        raise HTTPException(status_code=500, detail="Cache stats unavailable")


@app.post("/cache/clear")
@limiter.limit("10/minute")
async def clear_cache(
    request: Request, pattern: str = "*", api_key: str = Depends(verify_api_key)
):
    """Clear cache entries matching pattern (requires API key)"""
    try:
        logger.info(f"Cache clear requested by API key: {api_key[:8]}...")
        await cache_manager.clear_pattern(pattern)
        cache_manager.clear_l1()
        return {
            "status": "ok",
            "message": f"Cache cleared for pattern: {pattern}",
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Cache clear error: {e}")
        raise HTTPException(status_code=500, detail="Cache clear failed")


# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": True,
            "message": exc.detail,
            "timestamp": datetime.now().isoformat(),
            "path": request.url.path,
            "status_code": exc.status_code,
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(
        f"Unhandled exception on {request.url.path}: {exc}", exc_info=True
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": True,
            "message": "Internal server error",
            "timestamp": datetime.now().isoformat(),
            "path": request.url.path,
            "status_code": 500,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
