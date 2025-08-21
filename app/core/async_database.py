"""
Async database layer with AsyncPG and secure query building utilities
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, List, Dict, Any, Optional, Union
from datetime import datetime, timezone
import asyncio

import asyncpg
import redis.asyncio as aioredis
from shared.config import settings
from app.core.circuit_breaker import (
    with_questdb_protection,
    with_redis_protection,
)

logger = logging.getLogger(__name__)


# NOTE: QueryBuilder removed for simplicity - direct AsyncPG parameterized queries are more reliable


class AsyncQuestDBManager:
    """Async QuestDB connection manager with connection pooling"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self):
        """Initialize AsyncPG connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=settings.questdb_host,
                port=settings.questdb_pg_port,
                user=settings.questdb_user,
                password=settings.questdb_password,
                database=settings.questdb_database,
                min_size=settings.questdb_pool_min_conn,
                max_size=settings.questdb_pool_max_conn,
                command_timeout=60,
            )
            logger.info("AsyncPG connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize AsyncPG pool: {e}")
            raise

    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("AsyncPG connection pool closed")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Get connection from pool"""
        if not self.pool:
            raise RuntimeError("AsyncPG pool not initialized")

        async with self.pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                logger.error(f"Database error: {e}")
                raise

    async def fetch_all(self, query: str, *params) -> List[Dict[str, Any]]:
        """Execute query and fetch all results as dict list"""

        async def _fetch_operation():
            async with self.get_connection() as conn:
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]

        return await with_questdb_protection(_fetch_operation)

    async def fetch_one(self, query: str, *params) -> Optional[Dict[str, Any]]:
        """Execute query and fetch one result as dict"""

        async def _fetch_operation():
            async with self.get_connection() as conn:
                row = await conn.fetchrow(query, *params)
                return dict(row) if row else None

        return await with_questdb_protection(_fetch_operation)

    async def execute(self, query: str, *params) -> str:
        """Execute query and return status"""

        async def _execute_operation():
            async with self.get_connection() as conn:
                return await conn.execute(query, *params)

        return await with_questdb_protection(_execute_operation)

    async def fetch_one_timed(
        self, query: str, *params, timeout: Optional[float] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute query with timeout and fetch one result"""
        timeout = timeout or settings.db_query_timeout_sec

        try:
            return await asyncio.wait_for(
                self.fetch_one(query, *params), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Query timeout after {timeout}s: {query[:100]}...")
            raise

    async def fetch_all_timed(
        self, query: str, *params, timeout: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Execute query with timeout and fetch all results"""
        timeout = timeout or settings.db_query_timeout_sec

        try:
            return await asyncio.wait_for(
                self.fetch_all(query, *params), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Query timeout after {timeout}s: {query[:100]}...")
            raise

    async def execute_timed(
        self, query: str, *params, timeout: Optional[float] = None
    ) -> str:
        """Execute query with timeout"""
        timeout = timeout or settings.db_query_timeout_sec

        try:
            return await asyncio.wait_for(
                self.execute(query, *params), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Query timeout after {timeout}s: {query[:100]}...")
            raise


# QueryBuilder methods removed - use direct parameterized queries instead


class AsyncRedisManager:
    """Async Redis connection and geospatial operations manager"""

    def __init__(self):
        self.client: Optional[aioredis.Redis] = None
        self.geo_key = f"voila:{settings.redis_geo_key}"  # Namespace isolation

    async def initialize(self):
        """Initialize async Redis connection"""
        try:
            self.client = aioredis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                max_connections=settings.redis_connection_pool_size,
            )
            # Test connection
            await self.client.ping()
            logger.info("Async Redis connection established")
        except Exception as e:
            logger.error(f"Failed to connect to async Redis: {e}")
            raise

    async def close(self):
        """Close Redis connection"""
        if self.client:
            await self.client.close()
            logger.info("Async Redis connection closed")

    async def get_nearby_companies(
        self, lat: float, lng: float, radius_km: float, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get companies within radius using Redis GEORADIUS"""
        if not self.client:
            raise RuntimeError("Redis client not initialized")

        async def _redis_operation():
            # GEORADIUS with distance and coordinates
            nearby = await self.client.georadius(
                self.geo_key,
                lng,
                lat,
                radius_km,
                unit="km",
                withdist=True,
                withcoord=True,
                sort="ASC",
                count=limit,
            )

            results = []
            for item in nearby:
                ticker = item[0]
                distance_km = float(item[1])
                coords = item[2]  # [lng, lat]

                # Get company metadata from hash with namespace
                metadata = await self.client.hgetall(f"voila:company:{ticker}")

                results.append(
                    {
                        "ticker": ticker,
                        "distance_km": round(distance_km, 2),
                        "latitude": coords[1],
                        "longitude": coords[0],
                        "name": metadata.get("name", ""),
                        "address": metadata.get("address", ""),
                    }
                )

            return results

        try:
            return await with_redis_protection(_redis_operation)
        except Exception as e:
            logger.error(f"Redis geospatial query error: {e}")
            return []

    async def get_companies_in_region(
        self, coordinates: List[tuple[float, float]], limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get companies within a polygon using bounding box approach"""
        if not coordinates or len(coordinates) < 3:
            return []

        # Calculate bounding box
        lats = [coord[0] for coord in coordinates]
        lngs = [coord[1] for coord in coordinates]

        center_lat = sum(lats) / len(lats)
        center_lng = sum(lngs) / len(lngs)

        # Estimate radius to cover bounding box
        max_lat_diff = max(lats) - min(lats)
        max_lng_diff = max(lngs) - min(lngs)
        radius_km = max(max_lat_diff, max_lng_diff) * 111  # Rough km per degree

        return await self.get_nearby_companies(
            center_lat, center_lng, radius_km, limit
        )

    async def cache_set(
        self, key: str, value: str, ttl: Optional[int] = None
    ) -> bool:
        """Set cache value with optional TTL and namespace"""
        if not self.client:
            return False

        async def _cache_operation():
            namespaced_key = f"voila:cache:{key}"
            ttl_value = ttl or settings.redis_cache_ttl
            return await self.client.setex(namespaced_key, ttl_value, value)

        try:
            return await with_redis_protection(_cache_operation)
        except Exception as e:
            logger.error(f"Redis cache set error: {e}")
            return False

    async def cache_get(self, key: str) -> Optional[str]:
        """Get cached value with namespace"""
        if not self.client:
            return None

        async def _cache_operation():
            namespaced_key = f"voila:cache:{key}"
            return await self.client.get(namespaced_key)

        try:
            return await with_redis_protection(_cache_operation)
        except Exception as e:
            logger.error(f"Redis cache get error: {e}")
            return None

    async def get_company_metadata(
        self, ticker: str
    ) -> Optional[Dict[str, str]]:
        """Get company metadata from Redis hash"""
        if not self.client:
            return None

        async def _metadata_operation():
            metadata_key = f"voila:company:{ticker}"
            metadata = await self.client.hgetall(metadata_key)
            return metadata if metadata else None

        try:
            return await with_redis_protection(_metadata_operation)
        except Exception as e:
            logger.error(f"Redis company metadata error: {e}")
            return None


# Global instances
async_questdb_manager = AsyncQuestDBManager()
async_redis_manager = AsyncRedisManager()


@asynccontextmanager
async def get_async_questdb_connection():
    """Dependency for getting async QuestDB connection"""
    async with async_questdb_manager.get_connection() as conn:
        yield conn


def get_async_redis_client() -> AsyncRedisManager:
    """Dependency for getting async Redis client"""
    return async_redis_manager


# Utility functions for common patterns
def normalize_timestamp(ts: Union[datetime, str]) -> datetime:
    """Normalize timestamp to UTC naive datetime for QuestDB"""
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)

    if ts.tzinfo is None:
        # Assume UTC for naive timestamps
        return ts
    else:
        # Convert to UTC and make naive
        return ts.astimezone(timezone.utc).replace(tzinfo=None)


def build_date_filter(start_date=None, end_date=None, date_field="ts"):
    """Build date filter conditions for QueryBuilder"""
    conditions = []
    params = []

    if start_date:
        conditions.append(f"{date_field} >= ${len(params) + 1}")
        params.append(start_date)
    if end_date:
        conditions.append(f"{date_field} <= ${len(params) + 1}")
        params.append(end_date)

    return " AND ".join(conditions) if conditions else None, params
