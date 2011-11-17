from redis import Redis
from .proxy import conn
from .queue import Queue
from .worker import Worker

def use_redis(redis=None):
    if redis is None:
        redis = Redis()
    elif not isinstance(redis, Redis):
        raise TypeError('Argument redis should be a Redis instance.')
    conn.push(redis)

__all__ = ['conn', 'Queue', 'Worker', 'use_redis']
