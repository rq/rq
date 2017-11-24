# from .worker import Worker


workers_by_queue_key = 'rq:workers:%s'


def register(worker, pipeline=None):
    """
    Store worker key in Redis data structures so we can easily discover
    all active workers.
    """
    connection = pipeline if pipeline is not None else worker.connection
    connection.sadd(worker.redis_workers_keys, worker.key)
    for name in worker.queue_names():
        redis_key = workers_by_queue_key % name
        connection.sadd(redis_key, worker.key)


def unregister(worker, pipeline=None):
    """
    Remove worker key from Redis.
    """
    if pipeline is None:
        connection = worker.connection._pipeline()
    else:
        connection = pipeline
    
    connection.srem(worker.redis_workers_keys, worker.key)
    for name in worker.queue_names():
        redis_key = workers_by_queue_key % name
        connection.srem(redis_key, worker.key)

    if pipeline is None:
        connection.execute()
