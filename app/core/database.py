"""
Database connection managers for QuestDB and Redis
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import redis
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import DictCursor

from shared.config import settings

logger = logging.getLogger(__name__)


class QuestDBManager:
    """QuestDB connection pool manager"""

    def __init__(self):
        self.pool: Optional[ThreadedConnectionPool] = None

    def initialize(self):
        """Initialize QuestDB connection pool"""
        try:
            self.pool = ThreadedConnectionPool(
                minconn=settings.questdb_pool_min_conn,
                maxconn=settings.questdb_pool_max_conn,
                host=settings.questdb_host,
                port=settings.questdb_pg_port,
                user=settings.questdb_user,
                password=settings.questdb_password,
                database=settings.questdb_database,
                cursor_factory=DictCursor,
            )
            logger.info("QuestDB connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize QuestDB pool: {e}")
            raise

    def close(self):
        """Close QuestDB connection pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("QuestDB connection pool closed")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator:
        """Get connection from pool with context manager"""
        if not self.pool:
            raise RuntimeError("QuestDB pool not initialized")

        conn = None
        try:
            conn = self.pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                self.pool.putconn(conn)


class RedisManager:
    """Redis connection and geospatial operations manager"""

    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self.geo_key = f"voila:{settings.redis_geo_key}"  # Namespace isolation

    def initialize(self):
        """Initialize Redis connection"""
        try:
            self.client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                max_connections=settings.redis_connection_pool_size,
            )
            # Test connection
            self.client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def close(self):
        """Close Redis connection"""
        if self.client:
            self.client.close()
            logger.info("Redis connection closed")

    def get_nearby_companies(
        self, lat: float, lng: float, radius_km: float, limit: int = 100
    ) -> list[dict]:
        """Get companies within radius using Redis GEORADIUS"""
        if not self.client:
            raise RuntimeError("Redis client not initialized")

        try:
            # GEORADIUS with distance and coordinates
            nearby = self.client.georadius(
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
                metadata = self.client.hgetall(f"voila:company:{ticker}")

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

        except Exception as e:
            logger.error(f"Redis geospatial query error: {e}")
            return []

    def get_companies_in_region(
        self, coordinates: list[tuple[float, float]], limit: int = 1000
    ) -> list[dict]:
        """Get companies within a polygon using multiple GEORADIUS queries"""
        # For polygon queries, we'll use a bounding box approach with GEORADIUS
        # More complex polygon filtering would be done in application logic
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

        return self.get_nearby_companies(
            center_lat, center_lng, radius_km, limit
        )

    def cache_set(
        self, key: str, value: str, ttl: Optional[int] = None
    ) -> bool:
        """Set cache value with optional TTL"""
        if not self.client:
            return False

        try:
            ttl = ttl or settings.redis_cache_ttl
            namespaced_key = f"voila:cache:{key}"
            return self.client.setex(namespaced_key, ttl, value)
        except Exception as e:
            logger.error(f"Redis cache set error: {e}")
            return False

    def cache_get(self, key: str) -> Optional[str]:
        """Get cached value"""
        if not self.client:
            return None

        try:
            namespaced_key = f"voila:cache:{key}"
            return self.client.get(namespaced_key)
        except Exception as e:
            logger.error(f"Redis cache get error: {e}")
            return None


# Global instances
questdb_manager = QuestDBManager()
redis_manager = RedisManager()


@asynccontextmanager
async def get_questdb_connection():
    """Dependency for getting QuestDB connection"""
    async with questdb_manager.get_connection() as conn:
        yield conn


def get_redis_client() -> RedisManager:
    """Dependency for getting Redis client"""
    return redis_manager
