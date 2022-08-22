import typing as t

if t.TYPE_CHECKING:
    from redis import Redis
    from redis.client import Pipeline
    from .worker import Worker
    from .queue import Queue

from .compat import as_text

from rq.utils import split_list

WORKERS_BY_QUEUE_KEY = 'rq:workers:%s'
REDIS_WORKER_KEYS = 'rq:workers'
MAX_KEYS = 1000


def register(worker: 'Worker', pipeline: t.Optional['Pipeline'] = None):
    """
    Store worker key in Redis so we can easily discover active workers.

    Args:
        worker (Worker): The Worker
        pipeline (t.Optional[Pipeline], optional): The Redis Pipeline. Defaults to None.
    """
    connection = pipeline if pipeline is not None else worker.connection
    connection.sadd(worker.redis_workers_keys, worker.key)
    for name in worker.queue_names():
        redis_key = WORKERS_BY_QUEUE_KEY % name
        connection.sadd(redis_key, worker.key)


def unregister(worker: 'Worker', pipeline: t.Optional['Pipeline'] = None):
    """Remove Worker key from Redis

    Args:
        worker (Worker): The Worker
        pipeline (t.Optional[Pipeline], optional): Redis Pipeline. Defaults to None.
    """
    if pipeline is None:
        connection = worker.connection.pipeline()
    else:
        connection = pipeline

    connection.srem(worker.redis_workers_keys, worker.key)
    for name in worker.queue_names():
        redis_key = WORKERS_BY_QUEUE_KEY % name
        connection.srem(redis_key, worker.key)

    if pipeline is None:
        connection.execute()


def get_keys(queue: t.Optional['Queue'] = None, connection: t.Optional['Redis'] = None) -> t.Set[t.Any]:
    """Returns a list of worker keys for a given queue.

    Args:
        queue (t.Optional[&#39;Queue&#39;], optional): The Queue. Defaults to None.
        connection (t.Optional[&#39;Redis&#39;], optional): The Redis Connection. Defaults to None.

    Raises:
        ValueError: If no Queue or Connection is provided.

    Returns:
        set: A Set with keys.
    """
    if queue is None and connection is None:
        raise ValueError('"Queue" or "connection" argument is required')

    if queue:
        redis = queue.connection
        redis_key = WORKERS_BY_QUEUE_KEY % queue.name
    else:
        redis = connection  # type: ignore
        redis_key = REDIS_WORKER_KEYS

    return {as_text(key) for key in redis.smembers(redis_key)}


def clean_worker_registry(queue: 'Queue'):
    """Delete invalid worker keys in registry.

    Args:
        queue (Queue): The Queue
    """
    keys = list(get_keys(queue))

    with queue.connection.pipeline() as pipeline:

        for key in keys:
            pipeline.exists(key)
        results = pipeline.execute()

        invalid_keys = []

        for i, key_exists in enumerate(results):
            if not key_exists:
                invalid_keys.append(keys[i])

        if invalid_keys:
            for invalid_subset in split_list(invalid_keys, MAX_KEYS):
                pipeline.srem(WORKERS_BY_QUEUE_KEY % queue.name, *invalid_subset)
                pipeline.srem(REDIS_WORKER_KEYS, *invalid_subset)
                pipeline.execute()
