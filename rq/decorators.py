from functools import wraps
import typing as t

if t.TYPE_CHECKING:
    from redis import Redis
    from .job import Retry

from rq.compat import string_types

from .defaults import DEFAULT_RESULT_TTL
from .queue import Queue
from .utils import backend_class


class job:  # noqa
    queue_class = Queue

    def __init__(self, queue: 'Queue', connection: t.Optional['Redis'] = None, timeout=None,
                 result_ttl=DEFAULT_RESULT_TTL, ttl=None,
                 queue_class=None, depends_on: t.Optional[t.List[t.Any]] = None, at_front: t.Optional[bool] = None,
                 meta=None, description=None, failure_ttl=None, retry: t.Optional['Retry'] = None, on_failure=None,
                 on_success=None):
        """
        A decorator that adds a ``delay`` method to the decorated function,
        which in turn creates a RQ job when called. Accepts a required
        ``queue`` argument that can be either a ``Queue`` instance or a string
        denoting the queue name.  For example::

            ..codeblock:python::

                >>> @job(queue='default')
                >>> def simple_add(x, y):
                >>>    return x + y
                >>> ...
                >>> # Puts `simple_add` function into queue
                >>> simple_add.delay(1, 2)
        """
        self.queue = queue
        self.queue_class = backend_class(self, 'queue_class', override=queue_class)
        self.connection = connection
        self.timeout = timeout
        self.result_ttl = result_ttl
        self.ttl = ttl
        self.meta = meta
        self.depends_on = depends_on
        self.at_front = at_front
        self.description = description
        self.failure_ttl = failure_ttl
        self.retry = retry
        self.on_success = on_success
        self.on_failure = on_failure

    def __call__(self, f):
        @wraps(f)
        def delay(*args, **kwargs):
            if isinstance(self.queue, string_types):
                queue = self.queue_class(name=self.queue,
                                         connection=self.connection)
            else:
                queue = self.queue

            depends_on = kwargs.pop('depends_on', None)
            job_id = kwargs.pop('job_id', None)
            at_front = kwargs.pop('at_front', False)

            if not depends_on:
                depends_on = self.depends_on

            if not at_front:
                at_front = self.at_front

            return queue.enqueue_call(f, args=args, kwargs=kwargs,
                                      timeout=self.timeout, result_ttl=self.result_ttl,
                                      ttl=self.ttl, depends_on=depends_on, job_id=job_id, at_front=at_front,
                                      meta=self.meta, description=self.description, failure_ttl=self.failure_ttl,
                                      retry=self.retry, on_failure=self.on_failure, on_success=self.on_success)
        f.delay = delay
        return f
