import uuid
import sys
import warnings

from collections import namedtuple
from datetime import datetime, timezone

from redis import WatchError

from .compat import as_text, string_types, total_ordering
from .connections import resolve_connection
from .defaults import DEFAULT_RESULT_TTL
from .exceptions import DequeueTimeout, NoSuchJobError
from .job import Job, JobStatus, Dependency
from .serializers import resolve_serializer
from .utils import backend_class, get_version, import_attribute, parse_timeout, utcnow


def compact(lst):
    return [item for item in lst if item is not None]


class EnqueueData(namedtuple('EnqueueData', ["func", "args", "kwargs", "timeout",
                                             "result_ttl", "ttl", "failure_ttl",
                                             "description", "job_id",
                                             "at_front", "meta", "retry"])):
    """Helper type to use when calling enqueue_many
    NOTE: Does not support `depends_on` yet.
    """

    __slots__ = ()


@total_ordering
class Queue:
    job_class = Job
    DEFAULT_TIMEOUT = 180  # Default timeout seconds.
    redis_queue_namespace_prefix = 'rq:queue:'
    redis_queues_keys = 'rq:queues'

    @classmethod
    def all(cls, connection=None, job_class=None, serializer=None):
        """Returns an iterable of all Queues.
        """
        connection = resolve_connection(connection)

        def to_queue(queue_key):
            return cls.from_queue_key(as_text(queue_key),
                                      connection=connection,
                                      job_class=job_class, serializer=serializer)

        return [to_queue(rq_key)
                for rq_key in connection.smembers(cls.redis_queues_keys)
                if rq_key]

    @classmethod
    def from_queue_key(cls, queue_key, connection=None, job_class=None, serializer=None):
        """Returns a Queue instance, based on the naming conventions for naming
        the internal Redis keys.  Can be used to reverse-lookup Queues by their
        Redis keys.
        """
        prefix = cls.redis_queue_namespace_prefix
        if not queue_key.startswith(prefix):
            raise ValueError('Not a valid RQ queue key: {0}'.format(queue_key))
        name = queue_key[len(prefix):]
        return cls(name, connection=connection, job_class=job_class, serializer=serializer)

    def __init__(self, name='default', default_timeout=None, connection=None,
                 is_async=True, job_class=None, serializer=None, **kwargs):
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

        self.serializer = resolve_serializer(serializer)
        self.redis_server_version = None

    def __len__(self):
        return self.count

    def __nonzero__(self):
        return True

    def __bool__(self):
        return True

    def __iter__(self):
        yield self

    def get_redis_server_version(self):
        """Return Redis server version of connection"""
        if not self.redis_server_version:
            self.redis_server_version = get_version(self.connection)
        return self.redis_server_version

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
            job = self.job_class.fetch(job_id, connection=self.connection, serializer=self.serializer)
        except NoSuchJobError:
            self.remove(job_id)
        else:
            if job.origin == self.name:
                return job

    def get_job_position(self, job_or_id):
        """Returns the position of a job within the queue

        Using Redis before 6.0.6 and redis-py before 3.5.4 has a complexity of
        worse than O(N) and should not be used for very long job queues. Redis
        and redis-py version afterwards should support the LPOS command
        handling job positions within Redis c implementation.
        """
        job_id = job_or_id.id if isinstance(job_or_id, self.job_class) else job_or_id

        if self.get_redis_server_version() >= (6, 0, 6):
            try:
                return self.connection.lpos(self.key, job_id)
            except AttributeError:
                # not yet implemented by redis-py
                pass

        if job_id in self.job_ids:
            return self.job_ids.index(job_id)
        return None

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
        return FailedJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    @property
    def started_job_registry(self):
        """Returns this queue's StartedJobRegistry."""
        from rq.registry import StartedJobRegistry
        return StartedJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    @property
    def finished_job_registry(self):
        """Returns this queue's FinishedJobRegistry."""
        from rq.registry import FinishedJobRegistry
        # TODO: Why was job_class only ommited here before?  Was it intentional?
        return FinishedJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    @property
    def deferred_job_registry(self):
        """Returns this queue's DeferredJobRegistry."""
        from rq.registry import DeferredJobRegistry
        return DeferredJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    @property
    def scheduled_job_registry(self):
        """Returns this queue's ScheduledJobRegistry."""
        from rq.registry import ScheduledJobRegistry
        return ScheduledJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    @property
    def canceled_job_registry(self):
        """Returns this queue's CanceledJobRegistry."""
        from rq.registry import CanceledJobRegistry
        return CanceledJobRegistry(queue=self, job_class=self.job_class, serializer=self.serializer)

    def remove(self, job_or_id, pipeline=None):
        """Removes Job from queue, accepts either a Job instance or ID."""
        job_id = job_or_id.id if isinstance(job_or_id, self.job_class) else job_or_id

        if pipeline is not None:
            return pipeline.lrem(self.key, 1, job_id)

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
                   meta=None, status=JobStatus.QUEUED, retry=None, *,
                   on_success=None, on_failure=None):
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
            origin=self.name, meta=meta, serializer=self.serializer, on_success=on_success,
            on_failure=on_failure
        )

        if retry:
            job.retries_left = retry.max
            job.retry_intervals = retry.intervals

        if isinstance(depends_on, Dependency):
            job.allow_dependency_failures = depends_on.allow_failure

        return job

    def setup_dependencies(
        self,
        job,
        pipeline=None
    ):
        # If a _dependent_ job depends on any unfinished job, register all the
        # _dependent_ job's dependencies instead of enqueueing it.
        #
        # `Job#fetch_dependencies` sets WATCH on all dependencies. If
        # WatchError is raised in the when the pipeline is executed, that means
        # something else has modified either the set of dependencies or the
        # status of one of them. In this case, we simply retry.
        if len(job._dependency_ids) > 0:
            orig_status = job.get_status(refresh=False)
            pipe = pipeline if pipeline is not None else self.connection.pipeline()
            while True:
                try:
                    # Also calling watch even if caller
                    # passed in a pipeline since Queue#create_job
                    # is called from within this method.
                    pipe.watch(job.dependencies_key)

                    dependencies = job.fetch_dependencies(
                        watch=True,
                        pipeline=pipe
                    )

                    pipe.multi()

                    for dependency in dependencies:
                        if dependency.get_status(refresh=False) != JobStatus.FINISHED:
                            # NOTE: If the following code changes local variables, those values probably have
                            # to be set back to their original values in the handling of WatchError below!
                            job.set_status(JobStatus.DEFERRED, pipeline=pipe)
                            job.register_dependency(pipeline=pipe)
                            job.save(pipeline=pipe)
                            job.cleanup(ttl=job.ttl, pipeline=pipe)
                            if pipeline is None:
                                pipe.execute()
                            return job
                    break
                except WatchError:
                    if pipeline is None:
                        # The call to job.set_status(JobStatus.DEFERRED, pipeline=pipe) above has changed the
                        # internal "_status". We have to reset it to its original value (probably QUEUED), so
                        # if during the next run no unfinished dependencies exist anymore, the job gets
                        # enqueued correctly by enqueue_call().
                        job._status = orig_status
                        continue
                    else:
                        # if pipeline comes from caller, re-raise to them
                        raise
        elif pipeline is not None:
            pipeline.multi()  # Ensure pipeline in multi mode before returning to caller
        return job

    def enqueue_call(self, func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, failure_ttl=None, description=None,
                     depends_on=None, job_id=None, at_front=False, meta=None,
                     retry=None, on_success=None, on_failure=None, pipeline=None):
        """Creates a job to represent the delayed function call and enqueues it.

        It is much like `.enqueue()`, except that it takes the function's args
        and kwargs as explicit arguments.  Any kwargs passed to this function
        contain options for RQ itself.
        """

        job = self.create_job(
            func, args=args, kwargs=kwargs, result_ttl=result_ttl, ttl=ttl,
            failure_ttl=failure_ttl, description=description, depends_on=depends_on,
            job_id=job_id, meta=meta, status=JobStatus.QUEUED, timeout=timeout,
            retry=retry, on_success=on_success, on_failure=on_failure
        )

        job = self.setup_dependencies(
            job,
            pipeline=pipeline
        )
        # If we do not depend on an unfinished job, enqueue the job.
        if job.get_status(refresh=False) != JobStatus.DEFERRED:
            return self.enqueue_job(job, pipeline=pipeline, at_front=at_front)
        return job

    @staticmethod
    def prepare_data(func, args=None, kwargs=None, timeout=None,
                     result_ttl=None, ttl=None, failure_ttl=None,
                     description=None, job_id=None,
                     at_front=False, meta=None, retry=None):
        # Need this till support dropped for python_version < 3.7, where defaults can be specified for named tuples
        # And can keep this logic within EnqueueData
        return EnqueueData(
            func, args, kwargs, timeout,
            result_ttl, ttl, failure_ttl,
            description, job_id,
            at_front, meta, retry
        )

    def enqueue_many(
        self,
        job_datas,
        pipeline=None
    ):
        """
        Creates multiple jobs (created via `Queue.prepare_data` calls)
        to represent the delayed function calls and enqueues them.
        """
        pipe = pipeline if pipeline is not None else self.connection.pipeline()
        jobs = [
            self.enqueue_job(
                self.create_job(
                    job_data.func, args=job_data.args, kwargs=job_data.kwargs, result_ttl=job_data.result_ttl,
                    ttl=job_data.ttl,
                    failure_ttl=job_data.failure_ttl, description=job_data.description,
                    depends_on=None,
                    job_id=job_data.job_id, meta=job_data.meta, status=JobStatus.QUEUED,
                    timeout=job_data.timeout,
                    retry=job_data.retry
                ),
                pipeline=pipe,
                at_front=job_data.at_front
            )
            for job_data in job_datas
        ]
        if pipeline is None:
            pipe.execute()
        return jobs

    def run_job(self, job):
        job.perform()
        job.set_status(JobStatus.FINISHED)
        job.save(include_meta=False)
        job.cleanup(job.get_result_ttl(default_ttl=DEFAULT_RESULT_TTL))
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
        retry = kwargs.pop('retry', None)
        on_success = kwargs.pop('on_success', None)
        on_failure = kwargs.pop('on_failure', None)
        pipeline = kwargs.pop('pipeline', None)

        if 'args' in kwargs or 'kwargs' in kwargs:
            assert args == (), 'Extra positional arguments cannot be used when using explicit args and kwargs'  # noqa
            args = kwargs.pop('args', None)
            kwargs = kwargs.pop('kwargs', None)

        return (f, timeout, description, result_ttl, ttl, failure_ttl,
                depends_on, job_id, at_front, meta, retry, on_success, on_failure,
                pipeline, args, kwargs)

    def enqueue(self, f, *args, **kwargs):
        """Creates a job to represent the delayed function call and enqueues it."""

        (f, timeout, description, result_ttl, ttl, failure_ttl,
         depends_on, job_id, at_front, meta, retry, on_success,
         on_failure, pipeline, args, kwargs) = Queue.parse_args(f, *args, **kwargs)

        return self.enqueue_call(
            func=f, args=args, kwargs=kwargs, timeout=timeout,
            result_ttl=result_ttl, ttl=ttl, failure_ttl=failure_ttl,
            description=description, depends_on=depends_on, job_id=job_id,
            at_front=at_front, meta=meta, retry=retry, on_success=on_success, on_failure=on_failure,
            pipeline=pipeline
        )

    def enqueue_at(self, datetime, f, *args, **kwargs):
        """Schedules a job to be enqueued at specified time"""

        (f, timeout, description, result_ttl, ttl, failure_ttl,
         depends_on, job_id, at_front, meta, retry, on_success, on_failure,
         pipeline, args, kwargs) = Queue.parse_args(f, *args, **kwargs)
        job = self.create_job(f, status=JobStatus.SCHEDULED, args=args, kwargs=kwargs,
                              timeout=timeout, result_ttl=result_ttl, ttl=ttl,
                              failure_ttl=failure_ttl, description=description,
                              depends_on=depends_on, job_id=job_id, meta=meta, retry=retry,
                              on_success=on_success, on_failure=on_failure)

        return self.schedule_job(job, datetime, pipeline=pipeline)

    def schedule_job(self, job, datetime, pipeline=None):
        """Puts job on ScheduledJobRegistry"""
        from .registry import ScheduledJobRegistry
        registry = ScheduledJobRegistry(queue=self)

        pipe = pipeline if pipeline is not None else self.connection.pipeline()

        # Add Queue key set
        pipe.sadd(self.redis_queues_keys, self.key)
        job.save(pipeline=pipe)
        registry.schedule(job, datetime, pipeline=pipe)
        if pipeline is None:
            pipe.execute()
        return job

    def enqueue_in(self, time_delta, func, *args, **kwargs):
        """Schedules a job to be executed in a given `timedelta` object"""
        return self.enqueue_at(datetime.now(timezone.utc) + time_delta,
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
            job = self.run_sync(job)

        return job

    def run_sync(self, job):
        with self.connection.pipeline() as pipeline:
            job.prepare_for_execution('sync', pipeline)

        try:
            job = self.run_job(job)
        except:  # noqa
            job.set_status(JobStatus.FAILED)
            if job.failure_callback:
                job.failure_callback(job, self.connection, *sys.exc_info())
        else:
            if job.success_callback:
                job.success_callback(job, self.connection, job.result)

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

                dependent_job_ids = {as_text(_id)
                                     for _id in pipe.smembers(dependents_key)}

                # There's no dependents
                if not dependent_job_ids:
                    break

                jobs_to_enqueue = [
                    dependent_job for dependent_job
                    in self.job_class.fetch_many(
                        dependent_job_ids,
                        connection=self.connection,
                        serializer=self.serializer
                    ) if dependent_job and dependent_job.dependencies_are_met(
                        parent_job=job,
                        pipeline=pipe,
                    )
                ]

                pipe.multi()

                if not jobs_to_enqueue:
                    break

                for dependent in jobs_to_enqueue:
                    registry = DeferredJobRegistry(dependent.origin,
                                                   self.connection,
                                                   job_class=self.job_class,
                                                   serializer=self.serializer)
                    registry.remove(dependent, pipeline=pipe)
                    if dependent.origin == self.name:
                        self.enqueue_job(dependent, pipeline=pipe)
                    else:
                        queue = self.__class__(name=dependent.origin, connection=self.connection)
                        queue.enqueue_job(dependent, pipeline=pipe)

                # Only delete dependents_key if all dependents have been enqueued
                if len(jobs_to_enqueue) == len(dependent_job_ids):
                    pipe.delete(dependents_key)
                else:
                    enqueued_job_ids = [job.id for job in jobs_to_enqueue]
                    pipe.srem(dependents_key, *enqueued_job_ids)

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
    def dequeue_any(cls, queues, timeout, connection=None, job_class=None, serializer=None):
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
                                       job_class=job_class,
                                       serializer=serializer)
            try:
                job = job_class.fetch(job_id, connection=connection, serializer=serializer)
            except NoSuchJobError:
                # Silently pass on jobs that don't exist (anymore),
                # and continue in the look
                continue
            except Exception as e:
                # Attach queue information on the exception for improved error
                # reporting
                e.job_id = job_id
                e.queue = queue
                raise e
            return job, queue
        return None, None

    # Total ordering definition (the rest of the required Python methods are
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
