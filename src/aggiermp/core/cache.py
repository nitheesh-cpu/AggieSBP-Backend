"""
Redis caching utilities for the API.

Provides:
- Redis connection management
- @cached decorator for endpoint caching
- Cache invalidation helpers
"""

import hashlib
import json
import os
from functools import wraps
from typing import Any, Callable, Optional

import redis.asyncio as redis  # type: ignore[import-not-found]
from fastapi import Request

# Redis connection settings
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Default TTLs (in seconds)
TTL_SHORT = 300  # 5 minutes - for stats
TTL_SECTIONS = 600  # 10 minutes - for sections (is_open updates every 15min)
TTL_STANDARD = 3600  # 1 hour - general endpoints
TTL_LONG = 86400  # 24 hours - static data (courses, professors, departments)
TTL_15MIN = 900  # 15 minutes - for sections (is_open updates every 15min)
TTL_WEEK = 604800  # 1 week - for weekly data (e.g., course reviews)
TTL_MONTH = 2592000  # 1 month - for monthly data (e.g., course reviews)

# Global Redis client
_redis_client: Optional[redis.Redis] = None


async def get_redis() -> Optional[redis.Redis]:
    """Get or create Redis connection."""
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
            )
            # Test connection
            await _redis_client.ping()  # type: ignore[misc]
        except Exception as e:
            print(f"Redis connection failed: {e}")
            _redis_client = None
    return _redis_client


async def close_redis() -> None:
    """Close Redis connection."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        _redis_client = None


def _generate_cache_key(request: Request) -> str:
    """Generate a cache key from request path and query params."""
    # Include path and sorted query params
    path = request.url.path
    query_params = str(sorted(request.query_params.items()))

    # Create a hash for the key
    key_data = f"{path}:{query_params}"
    key_hash = hashlib.md5(key_data.encode()).hexdigest()[:16]

    return f"api:{path}:{key_hash}"


def cached(ttl: int = TTL_STANDARD) -> Callable:
    """
    Decorator to cache endpoint responses in Redis.

    Usage:
        @app.get("/courses")
        @cached(ttl=TTL_LONG)
        async def get_courses(request: Request, ...):
            ...

    Args:
        ttl: Time to live in seconds

    Returns:
        Decorated function with caching
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Get request from args or kwargs
            request: Optional[Request] = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            if not request:
                request = kwargs.get("request")

            if not request:
                # Can't cache without request
                return await func(*args, **kwargs)

            redis_client = await get_redis()
            if not redis_client:
                # Redis not available, skip caching
                result = await func(*args, **kwargs)
                return result

            cache_key = _generate_cache_key(request)

            try:
                # Check cache
                cached_data = await redis_client.get(cache_key)
                if cached_data:
                    # Cache hit
                    data = json.loads(cached_data)
                    # Add cache header via request state
                    request.state.cache_hit = True
                    request.state.cache_ttl = await redis_client.ttl(cache_key)
                    return data

                # Cache miss - execute function
                result = await func(*args, **kwargs)

                # Store in cache (only if result is JSON-serializable)
                try:
                    await redis_client.setex(
                        cache_key, ttl, json.dumps(result, default=str)
                    )
                except (TypeError, ValueError):
                    # Result not JSON serializable, skip caching
                    pass

                request.state.cache_hit = False
                return result

            except Exception as e:
                # On any Redis error, just execute the function
                print(f"Cache error: {e}")
                return await func(*args, **kwargs)

        return wrapper

    return decorator


async def invalidate_cache(pattern: str) -> int:
    """
    Invalidate cache entries matching a pattern.

    Args:
        pattern: Redis key pattern (e.g., "api:/courses*")

    Returns:
        Number of keys deleted
    """
    redis_client = await get_redis()
    if not redis_client:
        return 0

    try:
        keys = []
        async for key in redis_client.scan_iter(match=pattern):
            keys.append(key)

        if keys:
            deleted: int = await redis_client.delete(*keys)
            return deleted
        return 0
    except Exception as e:
        print(f"Cache invalidation error: {e}")
        return 0


async def clear_all_cache() -> int:
    """Clear all API cache entries."""
    return await invalidate_cache("api:*")


async def get_cache_stats() -> dict:
    """Get cache statistics."""
    redis_client = await get_redis()
    if not redis_client:
        return {"status": "disconnected"}

    try:
        info = await redis_client.info("memory")
        keys_count = 0
        async for _ in redis_client.scan_iter(match="api:*"):
            keys_count += 1

        return {
            "status": "connected",
            "cached_endpoints": keys_count,
            "used_memory": info.get("used_memory_human", "unknown"),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
