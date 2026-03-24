from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis import Redis

    from rq.worker import BaseWorker


WORKERS_SUSPENDED = 'rq:suspended'


def is_suspended(connection: Redis, worker: BaseWorker | None = None):
    """Checks whether a Worker is suspended on a given connection
    PS: pipeline returns a list of responses
    Ref: https://github.com/andymccurdy/redis-py#pipelines

    Args:
        connection (Redis): The Redis Connection
        worker (Optional[Worker], optional): The Worker. Defaults to None.
    """
    with connection.pipeline() as pipeline:
        if worker is not None:
            worker.heartbeat(pipeline=pipeline)
        pipeline.exists(WORKERS_SUSPENDED)
        return pipeline.execute()[-1]


def suspend(connection: Redis, ttl: int | None = None):
    """
    Suspends.
    TTL of 0 will invalidate right away.

    Args:
        connection (Redis): The Redis connection to use..
        ttl (Optional[int], optional): time to live in seconds. Defaults to `None`
    """
    connection.set(WORKERS_SUSPENDED, 1)
    if ttl is not None:
        connection.expire(WORKERS_SUSPENDED, ttl)


def resume(connection: Redis):
    """
    Resumes.

    Args:
        connection (Redis): The Redis connection to use..
    """
    return connection.delete(WORKERS_SUSPENDED)
