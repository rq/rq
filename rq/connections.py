from redis import Connection as RedisConnection
from redis import Redis


class NoRedisConnectionException(Exception):
    pass


# redis-py >= 8 may add RESP3 maintenance-notification handlers and derived
# connection metadata to connection_kwargs. Those values describe the current
# pool's internal state, and some contain locks, so RQ drops them before
# rebuilding a Redis connection in another process.
REDIS_RUNTIME_CONNECTION_KWARGS = (
    'maint_notifications_config',
    'maint_notifications_pool_handler',
    'event_dispatcher',
    'orig_host_address',
    'orig_socket_timeout',
    'orig_socket_connect_timeout',
)


def get_connection_kwargs(connection: Redis) -> dict:
    """Return pool kwargs suitable for rebuilding this Redis connection in a child process."""
    kwargs = connection.connection_pool.connection_kwargs.copy()
    for key in REDIS_RUNTIME_CONNECTION_KWARGS:
        kwargs.pop(key, None)
    # redis-py marks unset kwargs (e.g. socket_keepalive_options) with a sentinel object() that it
    # recognizes by identity. Pickling/repr across the process boundary creates a new object(), so
    # the child treats it as a real value and breaks; drop it so the child re-applies its default.
    return {key: value for key, value in kwargs.items() if type(value) is not object}


def parse_connection(connection: Redis) -> tuple[type[Redis], type[RedisConnection], dict]:
    connection_pool_kwargs = get_connection_kwargs(connection)
    connection_pool_class = connection.connection_pool.connection_class

    return connection.__class__, connection_pool_class, connection_pool_kwargs
