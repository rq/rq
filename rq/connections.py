from contextlib import contextmanager
import typing as t
import warnings
from redis import Redis

from .local import LocalStack, release_local


class NoRedisConnectionException(Exception):
    pass


@contextmanager
def Connection(connection: t.Optional['Redis'] = None):  # noqa
    """The context manager for handling connections in a clean way.
    It will push the connection to the LocalStack, and pop the connection
    when leaving the context
    Example:
        ..codeblock:python::

            with Connection():
                w = Worker()
                w.work()

    Args:
        connection (Optional[Redis], optional): A Redis Connection instance. Defaults to None.
    """
    warnings.warn("The Conneciton context manager is deprecated. Use the `connection` parameter instead.",
                    DeprecationWarning)
    if connection is None:
        connection = Redis()
    push_connection(connection)
    try:
        yield
    finally:
        popped = pop_connection()
        assert popped == connection, \
            'Unexpected Redis connection was popped off the stack. ' \
            'Check your Redis connection setup.'


def push_connection(redis: 'Redis'):
    """
    Pushes the given connection on the stack.

    Args:
        redis (Redis): A Redis connection
    """
    _connection_stack.push(redis)


def pop_connection() -> 'Redis':
    """
    Pops the topmost connection from the stack.

    Returns:
        redis (Redis): A Redis connection
    """
    return _connection_stack.pop()


def use_connection(redis: t.Optional['Redis'] = None):
    """
    Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.

    Args:
        redis (t.Optional[Redis], optional): A Redis Connection. Defaults to None.
    """
    assert len(_connection_stack) <= 1, \
        'You should not mix Connection contexts with use_connection()'
    release_local(_connection_stack)

    if redis is None:
        redis = Redis()
    push_connection(redis)


def get_current_connection() -> 'Redis':
    """
    Returns the current Redis connection (i.e. the topmost on the
    connection stack).

    Returns:
        Redis: A Redis Connection
    """
    return _connection_stack.top


def resolve_connection(connection: t.Optional['Redis'] = None) -> 'Redis':
    """
    Convenience function to resolve the given or the current connection.
    Raises an exception if it cannot resolve a connection now.

    Args:
        connection (t.Optional[Redis], optional): A Redis connection. Defaults to None.

    Raises:
        NoRedisConnectionException: If connection couldn't be resolved.

    Returns:
        Redis: A Redis Connection
    """

    if connection is not None:
        return connection

    connection = get_current_connection()
    if connection is None:
        raise NoRedisConnectionException('Could not resolve a Redis connection')
    return connection


_connection_stack = LocalStack()

__all__ = ['Connection', 'get_current_connection', 'push_connection',
           'pop_connection', 'use_connection']
