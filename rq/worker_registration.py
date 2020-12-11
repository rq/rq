from .compat import as_text

from rq.utils import split_list

WORKERS_BY_QUEUE_KEY = 'rq:workers:%s'
REDIS_WORKER_KEYS = 'rq:workers'
MAX_REMOVABLE_KEYS = 1000


def register(worker, pipeline=None):
    """Store worker key in Redis so we can easily discover active workers."""
    connection = pipeline if pipeline is not None else worker.connection
    connection.sadd(worker.redis_workers_keys, worker.key)
    for name in worker.queue_names():
        redis_key = WORKERS_BY_QUEUE_KEY % name
        connection.sadd(redis_key, worker.key)


def unregister(worker, pipeline=None):
    """Remove worker key from Redis."""
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


def get_keys(queue=None, connection=None):
    """Returnes a list of worker keys for a queue"""
    if queue is None and connection is None:
        raise ValueError('"queue" or "connection" argument is required')

    if queue:
        redis = queue.connection
        redis_key = WORKERS_BY_QUEUE_KEY % queue.name
    else:
        redis = connection
        redis_key = REDIS_WORKER_KEYS

    return {as_text(key) for key in redis.smembers(redis_key)}


def clean_worker_registry(queue):
    """Delete invalid worker keys in registry"""
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
            for invalid_subset in split_list(invalid_keys, MAX_REMOVABLE_KEYS):
                pipeline.srem(WORKERS_BY_QUEUE_KEY % queue.name, *invalid_subset)
                pipeline.srem(REDIS_WORKER_KEYS, *invalid_subset)
                pipeline.execute()
