from functools import wraps
from .queue import Queue
from .connections import resolve_connection


class job(object):

    def __init__(self, queue, connection=None, timeout=None):
        """A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a required
        ``queue`` argument that can be either a ``Queue`` instance or a string
        denoting the queue name.  For example:

            @job(queue='default')
            def simple_add(x, y):
                return x + y

            simple_add.delay(1, 2) # Puts simple_add function into queue
        """
        self.queue = queue
        self.connection = resolve_connection(connection)
        self.timeout = timeout

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, basestring):
                queue = Queue(name=self.queue, connection=self.connection)
            else:
                queue = self.queue
            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                    timeout=self.timeout)
        f.delay = delay
        return f
