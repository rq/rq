import times
from .connections import resolve_connection
from .job import Job, Status
from .exceptions import NoSuchJobError, UnpickleError, InvalidJobOperationError
from .compat import total_ordering


def get_failed_queue(connection=None):
    """Returns a handle to the special failed queue."""
    return FailedQueue(connection=connection)


def compact(lst):
    return [item for item in lst if item is not None]


@total_ordering
class Queue(object):
    redis_queue_namespace_prefix = 'rq:queue:'

    @classmethod
    def all(cls, connection=None):
        """Returns an iterable of all Queues.
        """
        prefix = cls.redis_queue_namespace_prefix
        connection = resolve_connection(connection)

        def to_queue(queue_key):
            return cls.from_queue_key(queue_key, connection=connection)
        return map(to_queue, connection.keys('%s*' % prefix))

    @classmethod
    def from_queue_key(cls, queue_key, connection=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: %s' % (queue_key,))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 async=True):
        self.connection = resolve_connection(connection)
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '%s%s' % (prefix, name)
        self._default_timeout = default_timeout
        self._async = async

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    def empty(self):
        """Removes all messages on the queue."""
        self.connection.delete(self.key)

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.connection.lrange(self.key, 0, -1)

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        def safe_fetch(job_id):
            try:
                job = Job.safe_fetch(job_id, connection=self.connection)
            except NoSuchJobError:
                return None
            except UnpickleError:
                return None
            return job

        return compact([safe_fetch(job_id) for job_id in self.job_ids])

    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return self.connection.llen(self.key)

    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guarantueeing FIFO semantics.
        """
        COMPACT_QUEUE = 'rq:queue:_compact'

        self.connection.rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = self.connection.lpop(COMPACT_QUEUE)
            if job_id is None:
                break
            if Job.exists(job_id, self.connection):
                self.connection.rpush(self.key, job_id)


    def push_job_id(self, job_id):  # noqa
        """Pushes a job ID on the corresponding Redis queue."""
        self.connection.rpush(self.key, job_id)

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None, result_ttl=None): #noqa
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """
        timeout = timeout or self._default_timeout
        job = Job.create(func, args, kwargs, connection=self.connection,
                         result_ttl=result_ttl, status=Status.QUEUED)
        return self.enqueue_job(job, timeout=timeout)

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues
        it.

        Expects the function to call, along with the arguments and keyword
        arguments.

        The function argument `f` may be any of the following:

        * A reference to a function
        * A reference to an object's instance method
        * A string, representing the location of a function (must be
          meaningful to the import context of the workers)
        """
        if not isinstance(f, basestring) and f.__module__ == '__main__':
            raise ValueError(
                    'Functions from the __main__ module cannot be processed '
                    'by workers.')

        # Detect explicit invocations, i.e. of the form:
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, timeout=30)
        timeout = None
        result_ttl = None
        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs.'  # noqa
            timeout = kwargs.pop('timeout', None)
            args = kwargs.pop('args', None)
            result_ttl = kwargs.pop('result_ttl', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl)

    def enqueue_job(self, job, timeout=None, set_meta_data=True):
        """Enqueues a job for delayed execution.

        When the `timeout` argument is sent, it will overrides the default
        timeout value of 180 seconds.  `timeout` may either be a string or
        integer.

        If the `set_meta_data` argument is `True` (default), it will update
        the properties `origin` and `enqueued_at`.

        If Queue is instantiated with async=False, job is executed immediately.
        """
        if set_meta_data:
            job.origin = self.name
            job.enqueued_at = times.now()

        if timeout:
            job.timeout = timeout  # _timeout_in_seconds(timeout)
        else:
            job.timeout = 180  # default
        job.save()

        if self._async:
            self.push_job_id(job.id)
        else:
            job.perform()
            job.save()
        return job

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""
        return self.connection.lpop(self.key)

    @classmethod
    def lpop(cls, queue_keys, blocking, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to wrap it
        this way.
        """
        connection = resolve_connection(connection)
        if blocking:
            queue_key, job_id = connection.blpop(queue_keys)
            return queue_key, job_id
        else:
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    def dequeue(self):
        """Dequeues the front-most job from this queue.

        Returns a Job instance, which can be executed or inspected.
        """
        job_id = self.pop_job_id()
        if job_id is None:
            return None
        try:
            job = Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError as e:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking itself recursively
            return self.dequeue()
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.queue = self
            raise e
        return job

    @classmethod
    def dequeue_any(cls, queues, blocking, connection=None):
        """Class method returning the Job instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `blocking` argument,
        either blocks execution of this function until new messages arrive on
        any of the queues, or returns None.
        """
        queue_keys = [q.key for q in queues]
        result = cls.lpop(queue_keys, blocking, connection=connection)
        if result is None:
            return None
        queue_key, job_id = result
        queue = cls.from_queue_key(queue_key, connection=connection)
        try:
            job = Job.fetch(job_id, connection=connection)
        except NoSuchJobError:
            # Silently pass on jobs that don't exist (anymore),
            # and continue by reinvoking the same function recursively
            return cls.dequeue_any(queues, blocking, connection=connection)
        except UnpickleError as e:
            # Attach queue information on the exception for improved error
            # reporting
            e.job_id = job_id
            e.queue = queue
            raise e
        return job, queue


    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):  # noqa
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects.')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)


    def __repr__(self):  # noqa
        return 'Queue(%r)' % (self.name,)

    def __str__(self):
        return '<Queue \'%s\'>' % (self.name,)


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__('failed', connection=connection)

    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue).

        This is different from normal job enqueueing, since certain meta data
        must not be overridden (e.g. `origin` or `enqueued_at`) and other meta
        data must be inserted (`ended_at` and `exc_info`).
        """
        job.ended_at = times.now()
        job.exc_info = exc_info
        return self.enqueue_job(job, timeout=job.timeout, set_meta_data=False)

    def requeue(self, job_id):
        """Requeues the job with the given job ID."""
        try:
            job = Job.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            self.connection.lrem(self.key, job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if self.connection.lrem(self.key, job.id) == 0:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

        job.exc_info = None
        q = Queue(job.origin, connection=self.connection)
        q.enqueue_job(job, timeout=job.timeout)
