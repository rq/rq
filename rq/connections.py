from contextlib import contextmanager
from redis import Redis


class NoRedisConnectionException(Exception):
    pass


class _RedisConnectionStack(object):
    def __init__(self):
        self.stack = []

    def _get_current_object(self):
        try:
            return self.stack[-1]
        except IndexError:
            msg = 'No Redis connection configured.'
            raise NoRedisConnectionException(msg)

    def pop(self):
        return self.stack.pop()

    def push(self, connection):
        self.stack.append(connection)

    def empty(self):
        del self.stack[:]

    def depth(self):
        return len(self.stack)

    def __getattr__(self, name):
        return getattr(self._get_current_object(), name)


_connection_stack = _RedisConnectionStack()


@contextmanager
def Connection(connection=None):
    if connection is None:
        connection = Redis()
    _connection_stack.push(connection)
    try:
        yield
    finally:
        popped = _connection_stack.pop()
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
    assert _connection_stack.depth() <= 1, \
            'You should not mix Connection contexts with use_connection().'
    _connection_stack.empty()

    if redis is None:
        redis = Redis()
    push_connection(redis)


def get_current_connection():
    """Returns the current Redis connection (i.e. the topmost on the
    connection stack).
    """
    return _connection_stack._get_current_object()


__all__ = ['Connection',
        'get_current_connection', 'push_connection', 'pop_connection',
        'use_connection']
