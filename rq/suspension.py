WORKERS_SUSPENDED = 'rq:suspended'


def is_suspended(connection):
    return connection.exists(WORKERS_SUSPENDED)


def suspend(connection, ttl=None):
    """ttl = time to live in seconds.  Default is no expiration
       Note:  If you pass in 0 it will invalidate right away
    """
    connection.set(WORKERS_SUSPENDED, 1)
    if ttl is not None:
        connection.expire(WORKERS_SUSPENDED, ttl)


def resume(connection):
    return connection.delete(WORKERS_SUSPENDED)
