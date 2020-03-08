# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import uuid
import warnings

from datetime import datetime

from redis import WatchError

from .compat import as_text, string_types, total_ordering, utc
from .connections import resolve_connection
from .defaults import DEFAULT_RESULT_TTL
from .exceptions import DequeueTimeout, NoSuchJobError, UnpickleError
from .job import Job, JobStatus
from .utils import backend_class, import_attribute, parse_timeout, utcnow


def compact(lst):
    return [item for item in lst if item is not None]


@total_ordering
class Queue(object):
    job_class = Job
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    @classmethod
    def all(cls, connection=None, job_class=None):
        """Returns an iterable of all Queues.
        """
        connection = resolve_connection(connection)

        def to_queue(queue_key):
            return cls.from_queue_key(as_text(queue_key),
                                      connection=connection,
                                      job_class=job_class)
        return [to_queue(rq_key)
                for rq_key in connection.smembers(cls.redis_queues_keys)
                if rq_key]

    @classmethod
    def from_queue_key(cls, queue_key, connection=None, job_class=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: {0}'.format(queue_key))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection, job_class=job_class)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 is_async=True, job_class=None, **kwargs):
        self.connection = resolve_connection(connection)
        prefix = self.redis_queue_namespace_prefix
        self.name = name
        self._key = '{0}{1}'.format(prefix, name)
        self._default_timeout = parse_timeout(default_timeout) or self.DEFAULT_TIMEOUT
        self._is_async = is_async

        if 'async' in kwargs:
            self._is_async = kwargs['async']
            warnings.warn('The `async` keyword is deprecated. Use `is_async` instead', DeprecationWarning)

        # override class attribute job_class if one was passed
        if job_class is not None:
            if isinstance(job_class, string_types):
                job_class = import_attribute(job_class)
            self.job_class = job_class

    def __len__(self):
        return self.count

    def __nonzero__(self):
        return True

    def __bool__(self):
        return True

    def __iter__(self):
        yield self

    @property
    def key(self):
        """Returns the Redis key for this Queue."""
        return self._key

    @property
    def registry_cleaning_key(self):
        """Redis key used to indicate this queue has been cleaned."""
        return 'rq:clean_registries:%s' % self.name

    def acquire_cleaning_lock(self):
        """Returns a boolean indicating whether a lock to clean this queue
        is acquired. A lock expires in 899 seconds (15 minutes - 1 second)
        """
        return self.connection.set(self.registry_cleaning_key, 1, nx=1, ex=899)

    def empty(self):
        """Removes all messages on the queue."""
        script = """
            local prefix = "{0}"
            local q = KEYS[1]
            local count = 0
            while true do
                local job_id = redis.call("lpop", q)
                if job_id == false then
                    break
                end

                -- Delete the relevant keys
                redis.call("del", prefix..job_id)
                redis.call("del", prefix..job_id..":dependents")
                count = count + 1
            end
            return count
        """.format(self.job_class.redis_job_namespace_prefix).encode("utf-8")
        script = self.connection.register_script(script)
        return script(keys=[self.key])

    def delete(self, delete_jobs=True):
        """Deletes the queue. If delete_jobs is true it removes all the associated messages on the queue first."""
        if delete_jobs:
            self.empty()

        with self.connection.pipeline() as pipeline:
            pipeline.srem(self.redis_queues_keys, self._key)
            pipeline.delete(self._key)
            pipeline.execute()

    def is_empty(self):
        """Returns whether the current queue is empty."""
        return self.count == 0

    @property
    def is_async(self):
        """Returns whether the current queue is async."""
        return bool(self._is_async)

    def fetch_job(self, job_id):
        try:
            job = self.job_class.fetch(job_id, connection=self.connection)
        except NoSuchJobError:
            self.remove(job_id)
        else:
            if job.origin == self.name:
                return job

    def get_job_ids(self, offset=0, length=-1):
        """Returns a slice of job IDs in the queue."""
        start = offset
        if length >= 0:
            end = offset + (length - 1)
        else:
            end = length
        return [as_text(job_id) for job_id in
                self.connection.lrange(self.key, start, end)]

    def get_jobs(self, offset=0, length=-1):
        """Returns a slice of jobs in the queue."""
        job_ids = self.get_job_ids(offset, length)
        return compact([self.fetch_job(job_id) for job_id in job_ids])

    @property
    def job_ids(self):
        """Returns a list of all job IDS in the queue."""
        return self.get_job_ids()

    @property
    def jobs(self):
        """Returns a list of all (valid) jobs in the queue."""
        return self.get_jobs()

    @property
    def count(self):
        """Returns a count of all messages in the queue."""
        return self.connection.llen(self.key)

    @property
    def failed_job_registry(self):
        """Returns this queue's FailedJobRegistry."""
        from rq.registry import FailedJobRegistry
        return FailedJobRegistry(queue=self, job_class=self.job_class)

    @property
    def started_job_registry(self):
        """Returns this queue's FailedJobRegistry."""
        from rq.registry import StartedJobRegistry
        return StartedJobRegistry(queue=self, job_class=self.job_class)

    @property
    def finished_job_registry(self):
        """Returns this queue's FailedJobRegistry."""
        from rq.registry import FinishedJobRegistry
        return FinishedJobRegistry(queue=self)

    @property
    def deferred_job_registry(self):
        """Returns this queue's FailedJobRegistry."""
        from rq.registry import DeferredJobRegistry
        return DeferredJobRegistry(queue=self, job_class=self.job_class)

    @property
    def scheduled_job_registry(self):
        """Returns this queue's FailedJobRegistry."""
        from rq.registry import ScheduledJobRegistry
        return ScheduledJobRegistry(queue=self, job_class=self.job_class)

    def remove(self, job_or_id, pipeline=None):
        """Removes Job from queue, accepts either a Job instance or ID."""
        job_id = job_or_id.id if isinstance(job_or_id, self.job_class) else job_or_id

        if pipeline is not None:
            pipeline.lrem(self.key, 1, job_id)
            return

        return self.connection.lrem(self.key, 1, job_id)

    def compact(self):
        """Removes all "dead" jobs from the queue by cycling through it, while
        guaranteeing FIFO semantics.
        """
        COMPACT_QUEUE = '{0}_compact:{1}'.format(
            self.redis_queue_namespace_prefix, uuid.uuid4())  # noqa

        self.connection.rename(self.key, COMPACT_QUEUE)
        while True:
            job_id = as_text(self.connection.lpop(COMPACT_QUEUE))
            if job_id is None:
                break
            if self.job_class.exists(job_id, self.connection):
                self.connection.rpush(self.key, job_id)

    def push_job_id(self, job_id, pipeline=None, at_front=False):
        """Pushes a job ID on the corresponding Redis queue.
        'at_front' allows you to push the job onto the front instead of the back of the queue"""
        connection = pipeline if pipeline is not None else self.connection
        if at_front:
            connection.lpush(self.key, job_id)
        else:
            connection.rpush(self.key, job_id)

    def create_job(self, func, args=None, kwargs=None, timeout=None,
                   result_ttl=None, ttl=None, failure_ttl=None,
                   description=None, depends_on=None, job_id=None,
                   meta=None, status=JobStatus.QUEUED):
        """Creates a job based on parameters given."""
        timeout = parse_timeout(timeout)

        if timeout is None:
            timeout = self._default_timeout
        elif timeout == 0:
            raise ValueError('0 timeout is not allowed. Use -1 for infinite timeout')

        result_ttl = parse_timeout(result_ttl)
        failure_ttl = parse_timeout(failure_ttl)

        ttl = parse_timeout(ttl)
        if ttl is not None and ttl <= 0:
            raise ValueError('Job ttl must be greater than 0')

        job = self.job_class.create(
            func, args=args, kwargs=kwargs, connection=self.connection,
            result_ttl=result_ttl, ttl=ttl, failure_ttl=failure_ttl,
            status=status, description=description,
            depends_on=depends_on, timeout=timeout, id=job_id,
            origin=self.name, meta=meta
        )

        return job

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, failure_ttl=None,
                     description=None, depends_on=None, job_id=None,
                     at_front=False, meta=None):
        """Creates a job to represent the delayed function call and enqueues
        it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        job = self.create_job(
            func, args=args, kwargs=kwargs, result_ttl=result_ttl, ttl=ttl,
            failure_ttl=failure_ttl, description=description, depends_on=depends_on,
            job_id=job_id, meta=meta, status=JobStatus.QUEUED, timeout=timeout,
        )

        # If a _dependent_ job depends on any unfinished job, register all the
        # _dependent_ job's dependencies instead of enqueueing it.
        #
        # `Job#fetch_dependencies` sets WATCH on all dependencies. If
        # WatchError is raised in the when the pipeline is executed, that means
        # something else has modified either the set of dependencies or the
        # status of one of them. In this case, we simply retry.
        if depends_on is not None:
            with self.connection.pipeline() as pipe:
                while True:
                    try:

                        pipe.watch(job.dependencies_key)

                        dependencies = job.fetch_dependencies(
                            watch=True,
                            pipeline=pipe
                        )

                        pipe.multi()

                        for dependency in dependencies:
                            if dependency.get_status(refresh=False) != JobStatus.FINISHED:
                                job.set_status(JobStatus.DEFERRED, pipeline=pipe)
                                job.register_dependency(pipeline=pipe)
                                job.save(pipeline=pipe)
                                job.cleanup(ttl=job.ttl, pipeline=pipe)
                                pipe.execute()
                                return job

                        break
                    except WatchError:
                        continue

        job = self.enqueue_job(job, at_front=at_front)
        return job

    def run_job(self, job):
        job.perform()
        job.set_status(JobStatus.FINISHED)
        job.save(include_meta=False)
        job.cleanup(DEFAULT_RESULT_TTL)
        return job

    @classmethod
    def parse_args(cls, f, *args, **kwargs):
        """
        Parses arguments passed to `queue.enqueue()` and `queue.enqueue_at()`

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
        #     q.enqueue(foo, args=(1, 2), kwargs={'a': 1}, job_timeout=30)
        timeout = kwargs.pop('job_timeout', None)
        description = kwargs.pop('description', None)
        result_ttl = kwargs.pop('result_ttl', None)
        ttl = kwargs.pop('ttl', None)
        failure_ttl = kwargs.pop('failure_ttl', None)
        depends_on = kwargs.pop('depends_on', None)
        job_id = kwargs.pop('job_id', None)
        at_front = kwargs.pop('at_front', False)
        meta = kwargs.pop('meta', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs'  # noqa
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return (f, timeout, description, result_ttl, ttl, failure_ttl,
                depends_on, job_id, at_front, meta, args, kwargs)

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues it."""

        (f, timeout, description, result_ttl, ttl, failure_ttl,
         depends_on, job_id, at_front, meta, args, kwargs) = Queue.parse_args(f, *args, **kwargs)
        
        return self.enqueue_call(
            func=f, args=args, kwargs=kwargs, timeout=timeout,
            result_ttl=result_ttl, ttl=ttl, failure_ttl=failure_ttl,
            description=description, depends_on=depends_on, job_id=job_id,
            at_front=at_front, meta=meta
        )

    def enqueue_at(self, datetime, f, *args, **kwargs):
        """Schedules a job to be enqueued at specified time"""
        from .registry import ScheduledJobRegistry

        (f, timeout, description, result_ttl, ttl, failure_ttl,
         depends_on, job_id, at_front, meta, args, kwargs) = Queue.parse_args(f, *args, **kwargs)
        job = self.create_job(f, status=JobStatus.SCHEDULED, args=args, kwargs=kwargs,
                              timeout=timeout, result_ttl=result_ttl, ttl=ttl,
                              failure_ttl=failure_ttl, description=description,
                              depends_on=depends_on, job_id=job_id, meta=meta)

        registry = ScheduledJobRegistry(queue=self)
        with self.connection.pipeline() as pipeline:
            job.save(pipeline=pipeline)
            registry.schedule(job, datetime, pipeline=pipeline)
            pipeline.execute()

        return job

    def enqueue_in(self, time_delta, func, *args, **kwargs):
        """Schedules a job to be executed in a given `timedelta` object"""
        return self.enqueue_at(datetime.now(utc) + time_delta,
                               func, *args, **kwargs)

    def enqueue_job(self, job, pipeline=None, at_front=False):
        """Enqueues a job for delayed execution.

        If Queue is instantiated with is_async=False, job is executed immediately.
        """
        pipe = pipeline if pipeline is not None else self.connection.pipeline()

        # Add Queue key set
        pipe.sadd(self.redis_queues_keys, self.key)
        job.set_status(JobStatus.QUEUED, pipeline=pipe)

        job.origin = self.name
        job.enqueued_at = utcnow()

        if job.timeout is None:
            job.timeout = self._default_timeout
        job.save(pipeline=pipe)
        job.cleanup(ttl=job.ttl, pipeline=pipe)

        if self._is_async:
            self.push_job_id(job.id, pipeline=pipe, at_front=at_front)

        if pipeline is None:
            pipe.execute()

        if not self._is_async:
            job = self.run_job(job)

        return job

    def enqueue_dependents(self, job, pipeline=None):
        """Enqueues all jobs in the given job's dependents set and clears it.

        When called without a pipeline, this method uses WATCH/MULTI/EXEC.
        If you pass a pipeline, only MULTI is called. The rest is up to the
        caller.
        """
        from .registry import DeferredJobRegistry

        pipe = pipeline if pipeline is not None else self.connection.pipeline()
        dependents_key = job.dependents_key

        while True:
            try:
                # if a pipeline is passed, the caller is responsible for calling WATCH
                # to ensure all jobs are enqueued
                if pipeline is None:
                    pipe.watch(dependents_key)

                dependent_jobs = [self.job_class.fetch(as_text(job_id), connection=self.connection)
                                  for job_id in pipe.smembers(dependents_key)]

                pipe.multi()

                for dependent in dependent_jobs:
                    registry = DeferredJobRegistry(dependent.origin,
                                                   self.connection,
                                                   job_class=self.job_class)
                    registry.remove(dependent, pipeline=pipe)
                    if dependent.origin == self.name:
                        self.enqueue_job(dependent, pipeline=pipe)
                    else:
                        queue = self.__class__(name=dependent.origin, connection=self.connection)
                        queue.enqueue_job(dependent, pipeline=pipe)

                pipe.delete(dependents_key)

                if pipeline is None:
                    pipe.execute()

                break
            except WatchError:
                if pipeline is None:
                    continue
                else:
                    # if the pipeline comes from the caller, we re-raise the
                    # exception as it it the responsibility of the caller to
                    # handle it
                    raise

    def pop_job_id(self):
        """Pops a given job ID from this Redis queue."""
        return as_text(self.connection.lpop(self.key))

    @classmethod
    def lpop(cls, queue_keys, timeout, connection=None):
        """Helper method.  Intermediate method to abstract away from some
        Redis API details, where LPOP accepts only a single key, whereas BLPOP
        accepts multiple.  So if we want the non-blocking LPOP, we need to
        iterate over all queues, do individual LPOPs, and return the result.

        Until Redis receives a specific method for this, we'll have to wrap it
        this way.

        The timeout parameter is interpreted as follows:
            None - non-blocking (return immediately)
             > 0 - maximum number of seconds to block
        """
        connection = resolve_connection(connection)
        if timeout is not None:  # blocking variant
            if timeout == 0:
                raise ValueError('RQ does not support indefinite timeouts. Please pick a timeout value > 0')
            result = connection.blpop(queue_keys, timeout)
            if result is None:
                raise DequeueTimeout(timeout, queue_keys)
            queue_key, job_id = result
            return queue_key, job_id
        else:  # non-blocking variant
            for queue_key in queue_keys:
                blob = connection.lpop(queue_key)
                if blob is not None:
                    return queue_key, blob
            return None

    @classmethod
    def dequeue_any(cls, queues, timeout, connection=None, job_class=None):
        """Class method returning the job_class instance at the front of the given
        set of Queues, where the order of the queues is important.

        When all of the Queues are empty, depending on the `timeout` argument,
        either blocks execution of this function for the duration of the
        timeout or until new messages arrive on any of the queues, or returns
        None.

        See the documentation of cls.lpop for the interpretation of timeout.
        """
        job_class = backend_class(cls, 'job_class', override=job_class)

        while True:
            queue_keys = [q.key for q in queues]
            result = cls.lpop(queue_keys, timeout, connection=connection)
            if result is None:
                return None
            queue_key, job_id = map(as_text, result)
            queue = cls.from_queue_key(queue_key,
                                       connection=connection,
                                       job_class=job_class)
            try:
                job = job_class.fetch(job_id, connection=connection)
            except NoSuchJobError:
                # Silently pass on jobs that don't exist (anymore),
                # and continue in the look
                continue
            except UnpickleError as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = queue
                raise e
            return job, queue
        return None, None

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

    def __hash__(self):  # pragma: no cover
        return hash(self.name)

    def __repr__(self):  # noqa  # pragma: no cover
        return '{0}({1!r})'.format(self.__class__.__name__, self.name)

    def __str__(self):
        return '<{0} {1}>'.format(self.__class__.__name__, self.name)
