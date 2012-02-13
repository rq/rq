from redis import Redis
from .proxy import conn
from .queue import Queue
from .worker import Worker
from .version import VERSION


def use_redis(redis=None):
    """Pushes the given Redis connection (a redis.Redis instance) onto the
    connection stack.  This is merely a helper function to easily start
    using RQ without having to know or understand the RQ connection stack.

    When given None as an argument, a (default) Redis connection to
    redis://localhost:6379 is set up.
    """
    if redis is None:
        redis = Redis()
    elif not isinstance(redis, Redis):
        raise TypeError('Argument redis should be a Redis instance.')
    conn.push(redis)

__all__ = ['conn', 'Queue', 'Worker', 'use_redis']
__version__ = VERSION
