class NoRedisConnectionException(Exception):
    pass


class RedisConnectionProxy(object):
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

    def push(self, db):
        self.stack.append(db)

    def __getattr__(self, name):
        return getattr(self._get_current_object(), name)


conn = RedisConnectionProxy()

__all__ = ['conn']
