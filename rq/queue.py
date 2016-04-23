# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from .compat import string_types, total_ordering, as_text
from .exceptions import InvalidJobOperationError, NoSuchJobError
from .job import Job, JobStatus
from .utils import import_attribute, utcnow, compact, transaction
from .keys import queue_key_from_name, QUEUES_KEY



@total_ordering
class Queue(object):
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.

    def __init__(self, name='default', default_timeout=None, connection=None,
                 async=True):
        from .connections import resolve_connection
        self.connection = resolve_connection(connection)
        self.name = name
        self._key = queue_key_from_name(name)
        self._default_timeout = default_timeout
        self._async = async

    def __len__(self):
        return self.count

    def __iter__(self):
        yield self

    @property
    def rq_conn(self):
        return self.connection

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    @transaction
    def empty(self):
        """Removes all messages on the queue."""
        job_ids = self.connection._lrange(self.key, 0, -1)
        jobs = []
        for job_id in job_ids:
            try:
                job = self.connection.get_job(job_id)
                jobs.append(job)
            except NoSuchJobError:
                pass

        for job in jobs:
            job.delete()

        self.connection._delete(self.key)
        return len(jobs)

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    @transaction
    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return [as_text(job_id) for job_id in
                self.connection._lrange(self.key, start, end)]

    @transaction
    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""
        job_ids = self.get_job_ids(offset, length)
        jobs = []
        for job_id in job_ids:
            try:
                jobs.append(self.connection.get_job(job_id))
            except NoSuchJobError:
                pass
        return jobs

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @property
    @transaction
    def count(self):
        """Returns a count of all messages in the queue."""
        return self.connection._llen(self.key)

    @transaction
    def remove(self, job_or_id):
        """Removes Job from queue, accepts either a Job instance or ID."""
        if isinstance(job_or_id, Job):
            job = job_or_id
        else:
            job = Job(connection=self.connection, id=job_or_id)

        try:
            job.refresh()
            job._delete()
        except NoSuchJobError:
            pass

        self._remove(job.id)

    def _remove(self, job_id):
        """Simple remove without touching job"""
        self.connection._lrem(self.key, 1, job_id)

    @transaction
    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """
        jobs = self.get_jobs()  # hides missing jobs
        self.connection._delete(self.key)
        self.connection._lpush(self.key, *[job.id for job in jobs])

    @transaction
    def push_job_id(self, job_id, at_front=False):
        """
        Pushes a job ID on the corresponding Redis queue.
        'at_front' allows you to push the job onto the front instead of the back
        of the queue
        """
        # Add Queue key set
        self.connection._sadd(QUEUES_KEY, self.key)
        if at_front:
            self.connection._lpush(self.key, job_id)
        else:
            self.connection._rpush(self.key, job_id)

    def pop_job_id(self, job_id):
        self.connection._lrem(self.key, 1, job_id)

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, description=None,
                     depends_on=None, at_front=False, meta=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.

        Design: Jobs keep track of both their children and their parents but
        the child is responsible for adding itself to the parent's children
        list, since the child will always come after.
        """
        # Create job in memory
        timeout = timeout or self._default_timeout
        job = self.connection.mkjob(func=func,
                                    args=args, kwargs=kwargs,
                                    result_ttl=result_ttl, ttl=ttl,
                                    status=JobStatus.DEFERRED,
                                    description=description,
                                    depends_on=depends_on, timeout=timeout,
                                    origin=self.name, meta=meta)

        job.save()
        if self._async:
            job._enqueue_or_deferr(at_front=at_front)
        else:
            assert len(job._unfinished_parents()) == 0
            job.perform()

        return job

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
        if not isinstance(f, string_types) and f.__module__ == '__main__':
            raise ValueError('Functions from the __main__ module cannot be processed '
                             'by workers')

        # Detect explicit invocations, i.e. of the form:
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, timeout=30)
        timeout = kwargs.pop('timeout', None)
        description = kwargs.pop('description', None)
        result_ttl = kwargs.pop('result_ttl', None)
        ttl = kwargs.pop('ttl', None)
        depends_on = kwargs.pop('depends_on', None)
        at_front = kwargs.pop('at_front', False)
        meta = kwargs.pop('meta', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs'  # noqa
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return self.enqueue_call(func=f, args=args, kwargs=kwargs,
                                 timeout=timeout, result_ttl=result_ttl, ttl=ttl,
                                 description=description, depends_on=depends_on,
                                 at_front=at_front, meta=meta)

    def dequeue(self, timeout=0):
        """
        Dequeue a single job. Unlike RQ we expect all jobs to exist

        :param queues: list of queue names or queue objects
        :param timeout: How long to wait for a job to appear on one of the
               queues (if they are initially empty). None waits forever.
        """
        result = self.connection.dequeue_any([self], timeout)
        if result:
            job, queue = result
            return job
        else:
            return None

    # Total ordering defition (the rest of the required Python methods are
    # auto-generated by the @total_ordering decorator)
    def __eq__(self, other):  # noqa
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects')
        return self.name == other.name

    def __lt__(self, other):
        if not isinstance(other, Queue):
            raise TypeError('Cannot compare queues to other objects')
        return self.name < other.name

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):  # noqa
        return 'Queue({0!r})'.format(self.name)

    def __str__(self):
        return '<Queue {0!r}>'.format(self.name)

    def clean_registries(self):
        """ Cleans StartedJobRegistry and FinishedJobRegistry of a queue. """
        registry = self.connection.get_finished_registry(name=self.name)
        registry.cleanup()
        registry = self.connection.get_started_registry(name=self.name)
        registry.cleanup()


class FailedQueue(Queue):
    def __init__(self, connection=None):
        super(FailedQueue, self).__init__(JobStatus.FAILED, connection=connection)

    @transaction
    def quarantine(self, job, exc_info):
        """Puts the given Job in quarantine (i.e. put it on the failed
        queue)."""
        job.ended_at = utcnow()
        job.exc_info = exc_info
        job.set_status(JobStatus.FAILED)
        job.save()

        self.push_job_id(job.id)
        return job

    @transaction
    def requeue(self, job_id):
        """Requeues the job with the given job ID.

        Note: It is not necessary to check parents becuase only jobs that have
        been put on the queue and tried can fail
        """
        job = self.connection.get_job(job_id)
        if job is None:
            self.connection._lrem(self.key, 1, job_id)
            return

        # Delete it from the failed queue (raise an error if that failed)
        if job.get_status() != JobStatus.FAILED:
            raise InvalidJobOperationError('Cannot requeue non-failed jobs')

        self.connection._lrem(self.key, 1, job_id)
        job.set_status(JobStatus.QUEUED)
        job.exc_info = None
        queue = self.connection.mkqueue(job.origin)
        queue.push_job_id(job.id)
