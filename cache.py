"""
Simple in-memory cache with TTL and request coalescing for reducing Google Sheets API calls.
Thread-safe and async-safe implementation for FastAPI.
"""
import time
import asyncio
from threading import Lock
from typing import Any, Optional, Dict, Tuple, Callable, Awaitable
from logger_config import setup_logging

logger = setup_logging("INFO")


class SimpleCache:
    """Thread-safe in-memory cache with TTL support and request coalescing"""

    def __init__(self, ttl_seconds: int = 300):
        """
        Initialize cache with specified TTL.

        Args:
            ttl_seconds: Time to live in seconds (default: 300 = 5 minutes)
        """
        self.ttl_seconds = ttl_seconds
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.lock = Lock()
        self.in_progress: Dict[str, asyncio.Lock] = {}
        self.progress_lock = Lock()
        logger.info(f"Cache initialized with TTL: {ttl_seconds}s ({ttl_seconds/60:.1f} minutes)")

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache if it exists and is not expired.

        Args:
            key: Cache key

        Returns:
            Cached value if fresh, None if expired or not found
        """
        with self.lock:
            if key not in self.cache:
                logger.debug(f"Cache MISS: '{key}' not found in cache")
                return None

            value, timestamp = self.cache[key]
            age = time.time() - timestamp

            if age > self.ttl_seconds:
                logger.info(f"Cache EXPIRED: '{key}' (age: {age:.1f}s, ttl: {self.ttl_seconds}s)")
                del self.cache[key]
                return None

            logger.info(f"Cache HIT: '{key}' (age: {age:.1f}s, ttl: {self.ttl_seconds}s)")
            return value

    def set(self, key: str, value: Any) -> None:
        """
        Set value in cache with current timestamp.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self.lock:
            self.cache[key] = (value, time.time())
            logger.info(f"Cache SET: '{key}' cached for {self.ttl_seconds}s")

    def clear(self, key: Optional[str] = None) -> None:
        """
        Clear cache entry or entire cache.

        Args:
            key: Specific key to clear, or None to clear all
        """
        with self.lock:
            if key is None:
                count = len(self.cache)
                self.cache.clear()
                logger.info(f"Cache CLEARED: All {count} entries removed")
            elif key in self.cache:
                del self.cache[key]
                logger.info(f"Cache CLEARED: '{key}' removed")

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries from cache.

        Returns:
            Number of entries removed
        """
        with self.lock:
            current_time = time.time()
            expired_keys = [
                key for key, (_, timestamp) in self.cache.items()
                if current_time - timestamp > self.ttl_seconds
            ]

            for key in expired_keys:
                del self.cache[key]

            if expired_keys:
                logger.info(f"Cache CLEANUP: Removed {len(expired_keys)} expired entries")

            return len(expired_keys)

    async def get_or_compute(self, key: str, compute_fn: Callable[[], Awaitable[Any]]) -> Any:
        """
        Get value from cache or compute it if missing. Prevents duplicate concurrent computations
        for the same key (request coalescing).

        Args:
            key: Cache key
            compute_fn: Async function to compute the value if not cached

        Returns:
            Cached or computed value
        """
        # First, try to get from cache
        cached_value = self.get(key)
        if cached_value is not None:
            return cached_value

        # Check if someone else is already computing this key
        with self.progress_lock:
            if key in self.in_progress:
                # Another request is already fetching this key
                lock = self.in_progress[key]
                logger.info(f"Request coalescing: Waiting for in-progress fetch of '{key}'")
            else:
                # We're the first request for this key, create a lock
                lock = asyncio.Lock()
                self.in_progress[key] = lock
                logger.info(f"Request coalescing: Starting new fetch for '{key}'")

        # Acquire the lock (either we created it or we're waiting for someone else)
        async with lock:
            # Double-check cache after acquiring lock (might have been populated while waiting)
            cached_value = self.get(key)
            if cached_value is not None:
                logger.info(f"Request coalescing: '{key}' was populated while waiting")
                # Clean up the in-progress lock if we're the last one
                with self.progress_lock:
                    if key in self.in_progress and not lock.locked():
                        del self.in_progress[key]
                return cached_value

            # Cache miss - compute the value
            try:
                logger.info(f"Request coalescing: Computing value for '{key}'")
                value = await compute_fn()
                self.set(key, value)
                return value
            finally:
                # Clean up the in-progress lock
                with self.progress_lock:
                    if key in self.in_progress:
                        del self.in_progress[key]
                        logger.info(f"Request coalescing: Completed fetch for '{key}'")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics for monitoring.

        Returns:
            Dictionary with cache stats
        """
        with self.lock:
            current_time = time.time()
            entries = []

            for key, (_, timestamp) in self.cache.items():
                age = current_time - timestamp
                entries.append({
                    'key': key,
                    'age_seconds': round(age, 1),
                    'ttl_remaining': round(self.ttl_seconds - age, 1),
                    'expired': age > self.ttl_seconds
                })

            return {
                'total_entries': len(self.cache),
                'ttl_seconds': self.ttl_seconds,
                'entries': entries
            }


# Global cache instance with 2-minute TTL
_cache = SimpleCache(ttl_seconds=120)


def get_cache() -> SimpleCache:
    """Get the global cache instance"""
    return _cache
