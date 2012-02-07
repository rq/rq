from datetime import datetime
from functools import total_ordering
from pickle import loads
from .proxy import conn
from .job import Job
from .exceptions import UnpickleError


class DelayedResult(object):
    """Proxy object that is returned as a result of `Queue.enqueue()` calls.
    Instances of DelayedResult can be polled for their return values.
    """
    def __init__(self, key):
        self.key = key
        self._rv = None

    @property
    def return_value(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be None.
        But when the job has been executed, and had a return value or exception,
        this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        DelayedResult object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_ been
        executed when its return value is None, since return values written back
        to Redis will expire after a given amount of time (500 seconds by
        default).
        """
        if self._rv is None:
            rv = conn.get(self.key)
            if rv is not None:
                # cache the result
                self._rv = loads(rv)
        return self._rv


@total_ordering
class Queue(object):
    redis_queue_namespace_prefix = 'rq:queue:'

    @classmethod
    def all(cls):
        """Returns an iterable of all Queues.
        """
        prefix = cls.redis_queue_namespace_prefix
        return map(cls.from_queue_key, conn.keys('%s*' % prefix))

    @classmethod
    def from_queue_key(cls, queue_key):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: %s' % (queue_key,))
        name = queue_key[len(prefix):]
        return Queue(name)

    def __init__(self, name='default'):
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '%s%s' % (prefix, name)

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    @property
    def messages(self):
        """Returns a list of all messages (pickled job data) in the queue."""
        return conn.lrange(self.key, 0, -1)

    @property
    def jobs(self):
        """Returns a list of all jobs in the queue."""
        return map(Job.unpickle, self.messages)

    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return conn.llen(self.key)


    def _create_job(self, f, *args, **kwargs):
        """Creates a Job instance for the given function call and attaches queue
        meta data to it.
        """
        if f.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed by workers.')

        job = Job(f, *args, **kwargs)
        job.origin = self.name
        return job

    def _push(self, pickled_job):
        """Enqueues a pickled_job on the corresponding Redis queue."""
        conn.rpush(self.key, pickled_job)

    def enqueue(self, f, *args, **kwargs):
        """Enqueues a function call for delayed execution.

        Expects the function to call, along with the arguments and keyword
        arguments.
        """
        job = self._create_job(f, *args, **kwargs)
        job.enqueued_at = datetime.utcnow()
        self._push(job.pickle())
        return DelayedResult(job.rv_key)

    def requeue(self, job):
        """Requeues an existing (typically a failed job) onto the queue."""
        raise NotImplementedError('Implement this')

    def dequeue(self):
        """Dequeues the function call at the front of this Queue.

        Returns a Job instance, which can be executed or inspected.
        """
        blob = conn.lpop(self.key)
        if blob is None:
            return None
        try:
            job = Job.unpickle(blob)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.queue = self
            raise e
        job.origin = self
        return job

    @classmethod
    def _lpop_any(cls, queue_keys):
        """Helper method.  You should not call this directly.

        Redis' BLPOP command takes multiple queue arguments, but LPOP can only
        take a single queue.  Therefore, we need to loop over all queues
        manually, in order, and return None if no more work is available.
        """
        for queue_key in queue_keys:
            blob = conn.lpop(queue_key)
            if blob is not None:
                return (queue_key, blob)
        return None

    @classmethod
    def dequeue_any(cls, queues, blocking):
        """Class method returning the Job instance at the front of the given set
        of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `blocking` argument,
        either blocks execution of this function until new messages arrive on
        any of the queues, or returns None.
        """
        queue_keys = map(lambda q: q.key, queues)
        if blocking:
            queue_key, blob = conn.blpop(queue_keys)
        else:
            redis_result = cls._lpop_any(queue_keys)
            if redis_result is None:
                return None
            queue_key, blob = redis_result

        queue = Queue.from_queue_key(queue_key)
        try:
            job = Job.unpickle(blob)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.queue = queue
            raise e
        job.origin = queue
        return job


    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)


    def __repr__(self):
        return 'Queue(%r)' % (self.name,)

    def __str__(self):
        return '<Queue \'%s\'>' % (self.name,)
