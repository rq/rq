from contextlib import contextmanager
from redis import Redis
from .local import LocalStack, release_local


class NoRedisConnectionException(Exception):
    pass


@contextmanager
def Connection(connection=None):
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


def push_connection(redis):
    """Pushes the given connection on the stack."""
    _connection_stack.push(redis)


def pop_connection():
    """Pops the topmost connection from the stack."""
    return _connection_stack.pop()


def use_connection(redis=None):
    """Clears the stack and uses the given connection.  Protects against mixed
    use of use_connection() and stacked connection contexts.
    """
    assert len(_connection_stack) <= 1, \
            'You should not mix Connection contexts with use_connection().'
    release_local(_connection_stack)

    if redis is None:
        redis = Redis()
    push_connection(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """
    return _connection_stack.top


_connection_stack = LocalStack()

__all__ = ['Connection',
        'get_current_connection', 'push_connection', 'pop_connection',
        'use_connection']
