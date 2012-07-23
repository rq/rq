from .connections import use_connection, get_current_connection
from .queue import Queue


class job(object):

    def __init__(self, queue=None):
        """
        A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a ``queue`` instance
        as an optional argument. For example:

            from rq import Queue, use_connection
            use_connection()
            q = Queue()

            @job(queue=q)
            def simple_add(x, y):
                return x + y

            simple_add.delay(1, 2) # Puts simple_add function into queue

        """
        self.queue = queue

    def __call__(self, f):
        def delay(*args, **kwargs):
            if self.queue is None:
                use_connection(get_current_connection())
                self.queue = Queue()
            return self.queue.enqueue(f, *args, **kwargs)
        f.delay = delay
        return f


