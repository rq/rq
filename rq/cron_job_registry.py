from __future__ import annotations

import time

from redis import Redis
from redis.client import Pipeline

from .defaults import DEFAULT_CRON_JOB_HISTORY_TTL


def get_registry_key() -> str:
    """Get the Redis key for the cron job name registry"""
    return 'rq:cron_jobs'


def add(name: str, connection: Redis | Pipeline, enqueue_timestamp: float | None = None) -> None:
    """Record a cron job name in the registry

    Adding a name that's already registered refreshes its timestamp. Multiple
    cron jobs may share a name, so duplicates are not an error.

    Args:
        name: Name of the cron job to record
        connection: Redis connection or pipeline to use
        enqueue_timestamp: When the cron job last enqueued. Defaults to the current time
    """
    connection.zadd(get_registry_key(), {name: enqueue_timestamp or time.time()})


def get_names(connection: Redis, cleanup: bool = True) -> list[str]:
    """Get all cron job names from the registry

    Args:
        connection: Redis connection to use
        cleanup: If True, removes stale entries from the registry before reading

    Returns:
        List of cron job names (strings) sorted by last enqueue time (oldest first)
    """
    if cleanup:
        remove_stale_entries(connection)

    names = connection.zrange(get_registry_key(), 0, -1)
    return [name.decode('utf-8') if isinstance(name, bytes) else name for name in names]


def remove_stale_entries(connection: Redis, threshold: int = DEFAULT_CRON_JOB_HISTORY_TTL) -> int:
    """Remove stale cron job names from the registry

    Removes names whose last enqueue is more than `threshold` seconds ago. The
    default threshold matches the job history TTL, so names age out of the
    registry on the same clock as the job history keys they point to.

    Args:
        connection: Redis connection to use
        threshold: Number of seconds after which an entry is considered stale

    Returns:
        Number of stale entries removed
    """
    cutoff_time = time.time() - threshold
    return connection.zremrangebyscore(get_registry_key(), 0, cutoff_time)
