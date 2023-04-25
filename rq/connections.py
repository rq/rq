import warnings
from contextlib import contextmanager
from typing import Any, Optional, Tuple, Type

from redis import Connection as RedisConnection, Redis, SSLConnection, UnixDomainSocketConnection

from .local import LocalStack


class NoRedisConnectionException(Exception):
    pass


@contextmanager
def Connection(connection: Optional['Redis'] = None):  # noqa
    """The context manager for handling connections in a clean way.
    It will push the connection to the LocalStack, and pop the connection
    when leaving the context

    Example:

    ..codeblock:python::

        with Connection():
            w = Worker()
            w.work()

    This method is deprecated on version 1.12.0 and will be removed in the future.
    Pass the connection to the worker explicitly to handle Redis Connections.

    Args:
        connection (Optional[Redis], optional): A Redis Connection instance. Defaults to None.
    """
    warnings.warn(
        "The Connection context manager is deprecated. Use the `connection` parameter instead.",
        DeprecationWarning,
    )
    if connection is None:
        connection = Redis()
    push_connection(connection)
    try:
        yield
    finally:
        popped = pop_connection()
        assert (
            popped == connection
        ), 'Unexpected Redis connection was popped off the stack. Check your Redis connection setup.'


def push_connection(redis: 'Redis'):
    """
    Pushes the given connection to the stack.

    Args:
        redis (Redis): A Redis connection
    """
    warnings.warn(
        "The `push_connection` function is deprecated. Pass the `connection` explicitly instead.",
        DeprecationWarning,
    )
    _connection_stack.push(redis)


def pop_connection() -> 'Redis':
    """
    Pops the topmost connection from the stack.

    Returns:
        redis (Redis): A Redis connection
    """
    warnings.warn(
        "The `pop_connection` function is deprecated. Pass the `connection` explicitly instead.",
        DeprecationWarning,
    )
    return _connection_stack.pop()


def get_current_connection() -> 'Redis':
    """
    Returns the current Redis connection (i.e. the topmost on the
    connection stack).

    Returns:
        Redis: A Redis Connection
    """
    warnings.warn(
        "The `get_current_connection` function is deprecated. Pass the `connection` explicitly instead.",
        DeprecationWarning,
    )
    return _connection_stack.top


def resolve_connection(connection: Optional['Redis'] = None) -> 'Redis':
    """
    Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.

    Args:
        connection (Optional[Redis], optional): A Redis connection. Defaults to None.

    Raises:
        NoRedisConnectionException: If connection couldn't be resolved.

    Returns:
        Redis: A Redis Connection
    """
    warnings.warn(
        "The `resolve_connection` function is deprecated. Pass the `connection` explicitly instead.",
        DeprecationWarning,
    )
    if connection is not None:
        return connection

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException('Could not resolve a Redis connection')
    return connection


def parse_connection(connection: Redis) -> Tuple[Type[Redis], Type[RedisConnection], dict]:
    connection_kwargs = connection.connection_pool.connection_kwargs.copy()
    # Redis does not accept parser_class argument which is sometimes present
    # on connection_pool kwargs, for example when hiredis is used
    connection_kwargs.pop('parser_class', None)
    connection_pool_class = connection.connection_pool.connection_class
    if issubclass(connection_pool_class, SSLConnection):
        connection_kwargs['ssl'] = True
    if issubclass(connection_pool_class, UnixDomainSocketConnection):
        # The connection keyword arguments are obtained from
        # `UnixDomainSocketConnection`, which expects `path`, but passed to
        # `redis.client.Redis`, which expects `unix_socket_path`, renaming
        # the key is necessary.
        # `path` is not left in the dictionary as that keyword argument is
        # not expected by `redis.client.Redis` and would raise an exception.
        connection_kwargs['unix_socket_path'] = connection_kwargs.pop('path')

    return connection.__class__, connection_pool_class, connection_kwargs


_connection_stack = LocalStack()


__all__ = ['Connection', 'get_current_connection', 'push_connection', 'pop_connection']
