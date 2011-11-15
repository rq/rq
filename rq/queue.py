import uuid
from pickle import loads, dumps
from .proxy import conn


class DelayedResult(object):
    def __init__(self, key):
        self.key = key
        self._rv = None

    @property
    def return_value(self):
        if self._rv is None:
            rv = conn.get(self.key)
            if rv is not None:
                # cache the result
                self._rv = loads(rv)
        return self._rv



def to_queue_key(queue_name):
    return 'rq:%s' % (queue_name,)


class Queue(object):
    def __init__(self, name='default'):
        self.name = name
        self._key = to_queue_key(name)

    @property
    def key(self):
        return self._key

    @property
    def empty(self):
        return self.count == 0

    @property
    def messages(self):
        return conn.lrange(self.key, 0, -1)

    @property
    def count(self):
        return conn.llen(self.key)

    def enqueue(self, job, *args, **kwargs):
        rv_key = '%s:result:%s' % (self.key, str(uuid.uuid4()))
        if job.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed by workers.')
        message = dumps((job, args, kwargs, rv_key))
        conn.rpush(self.key, message)
        return DelayedResult(rv_key)

    def dequeue(self):
        s = conn.lpop(self.key)
        return loads(s)

    def __str__(self):
        return self.name
