import time
from typing import TYPE_CHECKING, List, Optional

from redis import Redis
from redis.client import Pipeline

from .exceptions import DuplicateSchedulerError, SchedulerNotFound

if TYPE_CHECKING:
    from .cron import CronScheduler


def get_registry_key() -> str:
    """Get the Redis key for the CronScheduler registry"""
    return 'rq:cron_schedulers'


def register(cron_scheduler: 'CronScheduler', pipeline: Optional[Pipeline] = None) -> None:
    """Register a CronScheduler in the registry with current timestamp as score

    Args:
        cron_scheduler: CronScheduler instance to register
        pipeline: Redis pipeline to use. If None, uses cron_scheduler.connection

    Raises:
        DuplicateSchedulerError: If the scheduler is already registered
    """
    connection = pipeline if pipeline is not None else cron_scheduler.connection
    registry_key = get_registry_key()

    # Use current timestamp as score for sorting by registration/heartbeat time
    score = time.time()

    # Add to sorted set with scheduler name as member and timestamp as score
    # zadd with NX flag returns 0 if member already exists, 1 if added
    added_count = connection.zadd(registry_key, {cron_scheduler.name: score}, nx=True)
    if added_count == 0:
        raise DuplicateSchedulerError(f"CronScheduler '{cron_scheduler.name}' is already registered")


def unregister(cron_scheduler: 'CronScheduler', pipeline: Optional[Pipeline] = None) -> None:
    """Remove a CronScheduler from the registry

    Args:
        cron_scheduler: CronScheduler instance to unregister
        pipeline: Redis pipeline to use. If None, uses cron_scheduler.connection

    Raises:
        SchedulerNotFound: If the scheduler is not found in the registry
    """
    connection = pipeline if pipeline is not None else cron_scheduler.connection
    registry_key = get_registry_key()

    # Remove from sorted set - zrem returns number of elements removed
    result = connection.zrem(registry_key, cron_scheduler.name)
    if not result:
        raise SchedulerNotFound(f"CronScheduler '{cron_scheduler.name}' not found in registry")


def get_keys(connection: Redis) -> List[str]:
    """Get all registered CronScheduler names from the registry

    Args:
        connection: Redis connection to use

    Returns:
        List of CronScheduler names (strings) sorted by registration time (oldest first)
    """
    registry_key = get_registry_key()

    # Get all members from sorted set, ordered by score (registration time)
    # zrange returns bytes, so decode them to strings
    keys = connection.zrange(registry_key, 0, -1)

    # Decode bytes to strings
    return [key.decode('utf-8') if isinstance(key, bytes) else key for key in keys]


def cleanup(connection: Redis, threshold: int = 120) -> int:
    """Remove stale CronScheduler entries from the registry

    Removes schedulers that haven't sent a heartbeat in more than `threshold` seconds.

    Args:
        connection: Redis connection to use
        threshold: Number of seconds after which a scheduler is considered stale (default: 120)

    Returns:
        Number of stale entries removed
    """
    cutoff_time = time.time() - threshold

    # Remove entries with scores (timestamps) older than cutoff_time
    return connection.zremrangebyscore(get_registry_key(), 0, cutoff_time)
