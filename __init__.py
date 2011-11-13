import uuid
from pickle import loads, dumps
from rdb import conn

def to_queue_key(queue_name):
    return 'rq:%s' % (queue_name,)

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


class job(object):
    def __init__(self, queue='normal'):
        self.queue = queue

    def __call__(self, f):
        def delay(*args, **kwargs):
            queue_key = to_queue_key(self.queue)
            key = '%s:result:%s' % (queue_key, str(uuid.uuid4()))
            if f.__module__ == '__main__':
                raise ValueError('Functions from the __main__ module cannot be processed by workers.')
            s = dumps((f, key, args, kwargs))
            conn.rpush(queue_key, s)
            return DelayedResult(key)
        f.delay = delay
        return f



