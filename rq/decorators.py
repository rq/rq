from functools import wraps
from .queue import Queue
from .worker import DEFAULT_RESULT_TTL
from rq.compat import string_types


class job(object):
    def __init__(self, queue, connection=None, timeout=None,
                 result_ttl=DEFAULT_RESULT_TTL):
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
        self.connection = connection
        self.timeout = timeout
        self.result_ttl = result_ttl

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, string_types):
                queue = Queue(name=self.queue, connection=self.connection)
            else:
                queue = self.queue
            if 'depends_on' in kwargs:
                depends_on = kwargs.pop('depends_on')
            else:
                depends_on = None
            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                                      timeout=self.timeout, result_ttl=self.result_ttl, depends_on=depends_on)
        f.delay = delay
        return f
