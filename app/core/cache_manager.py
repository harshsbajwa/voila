"""
Multi-tier caching system with L1 (in-memory) and L2 (Redis) layers
"""

import json
import logging
import time
import hashlib
from typing import Any, Optional, Dict, Callable, Awaitable
from datetime import datetime
from functools import wraps
import asyncio

import redis.asyncio as aioredis
from shared.config import settings

logger = logging.getLogger(__name__)


class TTLCache:
    """In-memory cache with TTL support (L1 cache)"""

    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size

    def _is_expired(self, entry: Dict[str, Any]) -> bool:
        """Check if cache entry is expired"""
        return time.time() > entry["expires_at"]

    def _cleanup_expired(self):
        """Remove expired entries"""
        current_time = time.time()
        expired_keys = [
            key
            for key, entry in self.cache.items()
            if current_time > entry["expires_at"]
        ]
        for key in expired_keys:
            del self.cache[key]

    def _enforce_size_limit(self):
        """Enforce maximum cache size using LRU eviction"""
        if len(self.cache) >= self.max_size:
            # Remove oldest entries (simple LRU approximation)
            sorted_items = sorted(
                self.cache.items(), key=lambda x: x[1]["accessed_at"]
            )
            for key, _ in sorted_items[: len(sorted_items) // 4]:  # Remove 25%
                del self.cache[key]

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        self._cleanup_expired()

        if key in self.cache:
            entry = self.cache[key]
            if not self._is_expired(entry):
                entry["accessed_at"] = time.time()
                entry["hits"] += 1
                return entry["value"]
            else:
                del self.cache[key]

        return None

    def set(self, key: str, value: Any, ttl: int = 300):
        """Set value in cache with TTL"""
        self._cleanup_expired()
        self._enforce_size_limit()

        self.cache[key] = {
            "value": value,
            "expires_at": time.time() + ttl,
            "created_at": time.time(),
            "accessed_at": time.time(),
            "hits": 0,
        }

    def delete(self, key: str):
        """Remove key from cache"""
        self.cache.pop(key, None)

    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        self._cleanup_expired()
        total_hits = sum(entry["hits"] for entry in self.cache.values())
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "total_hits": total_hits,
            "hit_rate": total_hits / max(len(self.cache), 1),
        }


class AsyncCacheManager:
    """Multi-tier async cache manager with Redis and in-memory layers"""

    def __init__(self):
        self.l1_cache = TTLCache(max_size=1000)  # In-memory L1 cache
        self.redis_client: Optional[aioredis.Redis] = None
        self.cache_prefix = "voila:cache:v1"

    async def initialize(self):
        """Initialize Redis connection for L2 cache"""
        try:
            self.redis_client = aioredis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                max_connections=20,
            )
            await self.redis_client.ping()
            logger.info("Cache manager initialized with Redis L2 cache")
        except Exception as e:
            logger.warning(f"Redis L2 cache unavailable, using L1 only: {e}")
            self.redis_client = None

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()

    def _make_key(self, key: str) -> str:
        """Create namespaced cache key"""
        return f"{self.cache_prefix}:{key}"

    def _serialize_value(self, value: Any) -> str:
        """Serialize value for storage"""
        try:
            return json.dumps(value, default=str, separators=(",", ":"))
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to serialize cache value: {e}")
            return json.dumps(str(value))

    def _deserialize_value(self, serialized: str) -> Any:
        """Deserialize value from storage"""
        try:
            return json.loads(serialized)
        except (TypeError, ValueError) as e:
            logger.warning(f"Failed to deserialize cache value: {e}")
            return serialized

    async def get(self, key: str) -> Optional[Any]:
        """Get value from multi-tier cache (L1 -> L2 -> None)"""
        cache_key = self._make_key(key)

        # Try L1 cache first
        l1_value = self.l1_cache.get(cache_key)
        if l1_value is not None:
            return l1_value

        # Try L2 cache (Redis)
        if self.redis_client:
            try:
                l2_value = await self.redis_client.get(cache_key)
                if l2_value is not None:
                    deserialized = self._deserialize_value(l2_value)
                    # Warm L1 cache with shorter TTL
                    self.l1_cache.set(cache_key, deserialized, ttl=60)
                    return deserialized
            except Exception as e:
                logger.warning(f"Redis cache get failed: {e}")

        return None

    async def set(
        self, key: str, value: Any, ttl: int = 300, l1_ttl: Optional[int] = None
    ):
        """Set value in multi-tier cache"""
        cache_key = self._make_key(key)
        l1_ttl = l1_ttl or min(ttl, 60)  # L1 cache max 60 seconds

        # Set in L1 cache
        self.l1_cache.set(cache_key, value, ttl=l1_ttl)

        # Set in L2 cache (Redis)
        if self.redis_client:
            try:
                serialized = self._serialize_value(value)
                await self.redis_client.setex(cache_key, ttl, serialized)
            except Exception as e:
                logger.warning(f"Redis cache set failed: {e}")

    async def delete(self, key: str):
        """Delete key from all cache layers"""
        cache_key = self._make_key(key)

        # Delete from L1
        self.l1_cache.delete(cache_key)

        # Delete from L2
        if self.redis_client:
            try:
                await self.redis_client.delete(cache_key)
            except Exception as e:
                logger.warning(f"Redis cache delete failed: {e}")

    async def clear_pattern(self, pattern: str):
        """Clear cache entries matching pattern (L2 only)"""
        if self.redis_client:
            try:
                cache_pattern = self._make_key(pattern)
                keys = await self.redis_client.keys(cache_pattern)
                if keys:
                    await self.redis_client.delete(*keys)
                    logger.info(
                        f"Cleared {len(keys)} cache entries matching {pattern}"
                    )
            except Exception as e:
                logger.warning(f"Redis pattern clear failed: {e}")

    def clear_l1(self):
        """Clear L1 cache"""
        self.l1_cache.clear()

    async def stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        l1_stats = self.l1_cache.stats()

        redis_stats = {}
        if self.redis_client:
            try:
                info = await self.redis_client.info("memory")
                redis_stats = {
                    "memory_used": info.get("used_memory_human", "unknown"),
                    "connected": True,
                }
            except Exception as e:
                redis_stats = {"connected": False, "error": str(e)}
        else:
            redis_stats = {"connected": False, "error": "Not initialized"}

        return {
            "l1_cache": l1_stats,
            "l2_cache": redis_stats,
            "cache_prefix": self.cache_prefix,
        }


def cache_key_from_args(*args, **kwargs) -> str:
    """Generate cache key from function arguments"""
    # Create deterministic key from arguments
    key_parts = []

    # Add positional args
    for arg in args:
        if hasattr(arg, "model_dump"):  # Pydantic model
            key_parts.append(str(sorted(arg.model_dump().items())))
        else:
            key_parts.append(str(arg))

    # Add keyword args
    if kwargs:
        key_parts.append(str(sorted(kwargs.items())))

    # Hash to create manageable key
    key_string = "|".join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()


def cached(
    ttl: int = 300,
    l1_ttl: Optional[int] = None,
    key_prefix: str = "",
    skip_cache_on_error: bool = True,
):
    """Decorator for caching async function results"""

    def decorator(func: Callable[..., Awaitable[Any]]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Generate cache key
            func_name = getattr(func, "__name__", "unknown")
            func_key = f"{key_prefix}{func_name}:{cache_key_from_args(*args, **kwargs)}"

            # Try to get from cache
            cached_result = await cache_manager.get(func_key)
            if cached_result is not None:
                return cached_result

            # Execute function and cache result
            try:
                result = await func(*args, **kwargs)
                await cache_manager.set(
                    func_key, result, ttl=ttl, l1_ttl=l1_ttl
                )
                return result
            except Exception as e:
                if skip_cache_on_error:
                    raise
                # Cache the error for short time to prevent retry storms
                error_result = {"error": str(e), "cached_at": time.time()}
                await cache_manager.set(
                    func_key, error_result, ttl=30, l1_ttl=10
                )
                raise

        return wrapper

    return decorator


# Global cache manager instance
cache_manager = AsyncCacheManager()


# Cache warming functions
async def warm_market_overview_cache():
    """Warm frequently accessed market overview cache"""
    try:
        # Directly warm cache by calling the database layer
        from app.core.async_database import (
            async_questdb_manager,
            async_redis_manager,
        )
        from shared.config import settings
        import json

        logger.info("Warming market overview cache...")

        # Replicate market overview logic for cache warming
        cache_key = "market:overview"

        # 1) Latest market data per ticker
        latest_query = f"SELECT ticker, close, volume FROM {settings.questdb_ohlcv_table} LATEST BY ticker"
        latest_rows = await async_questdb_manager.fetch_all(latest_query)

        # 2) Company states
        companies_query = f"SELECT ticker, state FROM {settings.questdb_companies_table} WHERE state IS NOT NULL"
        company_rows = await async_questdb_manager.fetch_all(companies_query)

        # Create complete overview data matching endpoint logic
        ticker_to_state = {row["ticker"]: row["state"] for row in company_rows}

        # Aggregate geographical distribution
        state_to_stats = {}
        total_close = 0.0
        total_volume = 0
        total_companies = 0

        for row in latest_rows:
            ticker = row["ticker"]
            close = float(row["close"]) if row["close"] is not None else None
            volume = int(row["volume"]) if row["volume"] is not None else 0

            if ticker in ticker_to_state and close is not None:
                state = ticker_to_state[ticker]
                if state not in state_to_stats:
                    state_to_stats[state] = {
                        "state": state,
                        "company_count": 0,
                        "_sum_close": 0.0,
                        "total_volume": 0,
                    }
                s = state_to_stats[state]
                s["company_count"] += 1
                s["_sum_close"] += close
                s["total_volume"] += volume

            if close is not None:
                total_companies += 1
                total_close += close
                total_volume += volume

        # Finalize geographical distribution
        geographical_distribution = []
        for s in state_to_stats.values():
            avg_price = (
                round(s["_sum_close"] / s["company_count"], 2)
                if s["company_count"] > 0
                else None
            )
            geographical_distribution.append(
                {
                    "state": s["state"],
                    "company_count": s["company_count"],
                    "avg_price": avg_price,
                    "total_volume": s["total_volume"],
                }
            )

        geographical_distribution.sort(
            key=lambda x: x["company_count"], reverse=True
        )

        market_summary = {
            "total_companies": total_companies,
            "market_avg_price": (total_close / total_companies)
            if total_companies > 0
            else None,
            "total_volume": total_volume,
        }

        overview = {
            "market_summary": market_summary,
            "geographical_distribution": geographical_distribution,
            "timestamp": datetime.now().isoformat(),
        }

        # Cache for 10 minutes
        await async_redis_manager.cache_set(
            cache_key, json.dumps(overview, default=str), ttl=600
        )

        logger.info("Market overview cache warmed successfully")
    except Exception as e:
        logger.warning(f"Failed to warm market overview cache: {e}")


async def warm_popular_tickers_cache():
    """Warm cache for popular tickers"""
    try:
        from app.core.async_database import (
            async_questdb_manager,
            async_redis_manager,
        )
        from shared.config import settings
        import json
        from shared.models import OHLCVResponseRecord

        popular_tickers = [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "TSLA",
            "META",
            "NVDA",
        ]

        logger.info(
            f"Warming cache for {len(popular_tickers)} popular tickers..."
        )

        for ticker in popular_tickers:
            try:
                # Directly query and cache latest data
                cache_key = f"latest:{ticker}"

                query = f"""
                    SELECT ts as "Date", open as "Open", high as "High",
                           low as "Low", close as "Close", volume as "Volume"
                    FROM {settings.questdb_ohlcv_table}
                    WHERE ticker = $1
                    ORDER BY ts DESC
                    LIMIT 1
                """

                result = await async_questdb_manager.fetch_one(query, ticker)

                if result:
                    response = OHLCVResponseRecord(
                        Date=result["Date"],
                        Open=float(result["Open"]),
                        High=float(result["High"]),
                        Low=float(result["Low"]),
                        Close=float(result["Close"]),
                        Volume=int(result["Volume"]),
                    )

                    # Cache the result
                    await async_redis_manager.cache_set(
                        cache_key,
                        json.dumps(response.model_dump(), default=str),
                        ttl=300,
                    )

            except Exception as e:
                logger.warning(f"Failed to warm cache for {ticker}: {e}")

        logger.info("Popular tickers cache warmed successfully")
    except Exception as e:
        logger.warning(f"Failed to warm popular tickers cache: {e}")


async def cache_warming_job():
    """Background job for cache warming"""
    while True:
        try:
            await warm_market_overview_cache()
            await warm_popular_tickers_cache()

            # Sleep for 5 minutes before next warming
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            logger.info("Cache warming job cancelled")
            break
        except Exception as e:
            logger.error(f"Cache warming job error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error


# Cache invalidation strategies
class CacheInvalidator:
    """Handles cache invalidation strategies"""

    @staticmethod
    async def invalidate_ticker_data(ticker: str):
        """Invalidate all cache entries for a specific ticker"""
        patterns = [
            f"*latest:{ticker}*",
            f"*ohlcv:{ticker}*",
            f"*complete:{ticker}*",
            f"*market_data*{ticker}*",
        ]

        for pattern in patterns:
            await cache_manager.clear_pattern(pattern)

        logger.info(f"Invalidated cache for ticker: {ticker}")

    @staticmethod
    async def invalidate_market_overview():
        """Invalidate market overview cache"""
        await cache_manager.delete("market:overview")
        logger.info("Invalidated market overview cache")

    @staticmethod
    async def invalidate_geospatial_data():
        """Invalidate geospatial query cache"""
        patterns = ["*spatial*", "*companies_in*", "*nearby*"]

        for pattern in patterns:
            await cache_manager.clear_pattern(pattern)

        logger.info("Invalidated geospatial cache")


# Export for use in endpoints
invalidate_cache = CacheInvalidator()
