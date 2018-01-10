from .compat import as_text


WORKERS_BY_QUEUE_KEY = 'rq:workers:%s'
REDIS_WORKER_KEYS = 'rq:workers'


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
        connection = worker.connection._pipeline()
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
