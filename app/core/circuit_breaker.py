"""
Simple circuit breaker for critical operations
"""

import time
import logging
import asyncio
import os
import errno
from typing import Callable, Any
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing recovery


class SimpleCircuitBreaker:
    """Simple in-memory circuit breaker"""

    def __init__(
        self, name: str, failure_threshold: int = 5, recovery_timeout: int = 60
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.last_failure_time = None
        self.transient_error_codes = {
            errno.EMFILE,
            errno.EAGAIN,
            errno.ENFILE,
        }  # 24, 11, 23

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset"""
        if self.state != CircuitState.OPEN or not self.last_failure_time:
            return False
        return time.time() - self.last_failure_time >= self.recovery_timeout

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        # Check if we should attempt reset
        if self._should_attempt_reset():
            self.state = CircuitState.HALF_OPEN
            logger.info(f"Circuit breaker '{self.name}' attempting reset")

        # Block requests if circuit is open
        if self.state == CircuitState.OPEN:
            raise Exception(f"Circuit breaker '{self.name}' is OPEN")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(*args, **kwargs), timeout=30
                )
            else:
                result = func(*args, **kwargs)

            # Success - reset failures and close circuit
            self.consecutive_failures = 0
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                logger.info(f"Circuit breaker '{self.name}' reset to CLOSED")

            return result

        except Exception as e:
            # Check if it's a transient error that shouldn't count toward circuit opening
            is_transient = False

            if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
                is_transient = True
                logger.debug(
                    f"Circuit breaker '{self.name}': Timeout error (transient)"
                )
            elif isinstance(e, OSError) and hasattr(e, "errno"):
                if e.errno in self.transient_error_codes:
                    is_transient = True
                    logger.debug(
                        f"Circuit breaker '{self.name}': OS error {e.errno} (transient)"
                    )

            if not is_transient:
                self.consecutive_failures += 1
                self.last_failure_time = time.time()

                if self.consecutive_failures >= self.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(
                        f"Circuit breaker '{self.name}' opened after {self.consecutive_failures} failures"
                    )

            raise


# Simple global circuit breakers with configurable thresholds
questdb_threshold = int(os.getenv("VOILA_CB_QUESTDB_THRESHOLD", "20"))
questdb_timeout = int(os.getenv("VOILA_CB_QUESTDB_TIMEOUT", "15"))
redis_threshold = int(os.getenv("VOILA_CB_REDIS_THRESHOLD", "10"))
redis_timeout = int(os.getenv("VOILA_CB_REDIS_TIMEOUT", "30"))

questdb_circuit_breaker = SimpleCircuitBreaker(
    "questdb",
    failure_threshold=questdb_threshold,
    recovery_timeout=questdb_timeout,
)
redis_circuit_breaker = SimpleCircuitBreaker(
    "redis", failure_threshold=redis_threshold, recovery_timeout=redis_timeout
)


# Helper functions
async def with_questdb_protection(func, *args, **kwargs):
    """Execute database operation with circuit breaker"""
    return await questdb_circuit_breaker.call(func, *args, **kwargs)


async def with_redis_protection(func, *args, **kwargs):
    """Execute Redis operation with circuit breaker"""
    return await redis_circuit_breaker.call(func, *args, **kwargs)


# Simple decorator
def circuit_protected(breaker_name: str = "default"):
    """Decorator for circuit breaker protection"""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if breaker_name == "questdb":
                return await with_questdb_protection(func, *args, **kwargs)
            elif breaker_name == "redis":
                return await with_redis_protection(func, *args, **kwargs)
            else:
                return await func(*args, **kwargs)

        return wrapper

    return decorator
