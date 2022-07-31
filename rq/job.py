import inspect
import json
import pickle
import warnings
import zlib

import asyncio
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from uuid import uuid4

from redis import WatchError

from rq.compat import as_text, decode_redis_hash, string_types
from .connections import resolve_connection
from .exceptions import DeserializationError, InvalidJobOperation, NoSuchJobError
from .local import LocalStack
from .serializers import resolve_serializer
from .utils import (get_version, import_attribute, parse_timeout, str_to_date,
                    utcformat, utcnow, ensure_list, get_call_string)

# Serialize pickle dumps using the highest pickle protocol (binary, default
# uses ascii)
dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


class JobStatus(str, Enum):
    QUEUED = 'queued'
    FINISHED = 'finished'
    FAILED = 'failed'
    STARTED = 'started'
    DEFERRED = 'deferred'
    SCHEDULED = 'scheduled'
    STOPPED = 'stopped'
    CANCELED = 'canceled'


class Dependency:
    def __init__(self, jobs, allow_failure: bool = False):
        jobs = ensure_list(jobs)
        if not all(
            isinstance(job, Job) or isinstance(job, str)
            for job in jobs
            if job
        ):
            raise ValueError("jobs: must contain objects of type Job and/or strings representing Job ids")
        elif len(jobs) < 1:
            raise ValueError("jobs: cannot be empty.")

        self.dependencies = jobs
        self.allow_failure = allow_failure


# Sentinel value to mark that some of our lazily evaluated properties have not
# yet been evaluated.
UNEVALUATED = object()


def cancel_job(job_id, connection=None, serializer=None, enqueue_dependents=False):
    """Cancels the job with the given job ID, preventing execution.  Discards
    any job info (i.e. it can't be requeued later).
    """
    Job.fetch(job_id, connection=connection, serializer=serializer).cancel(enqueue_dependents=enqueue_dependents)


def get_current_job(connection=None, job_class=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    if job_class:
        warnings.warn("job_class argument for get_current_job is deprecated.",
                      DeprecationWarning)
    return _job_stack.top


def requeue_job(job_id, connection, serializer=None):
    job = Job.fetch(job_id, connection=connection, serializer=serializer)
    return job.requeue()


class Job:
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """
    redis_job_namespace_prefix = 'rq:job:'

    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None,
               result_ttl=None, ttl=None, status=None, description=None,
               depends_on=None, timeout=None, id=None, origin=None, meta=None,
               failure_ttl=None, serializer=None, *, on_success=None, on_failure=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict'.format(kwargs))

        job = cls(connection=connection, serializer=serializer)
        if id is not None:
            job.set_id(id)

        if origin is not None:
            job.origin = origin

        # Set the core job tuple properties
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '{0}.{1}'.format(func.__module__, func.__qualname__)
        elif isinstance(func, string_types):
            job._func_name = as_text(func)
        elif not inspect.isclass(func) and hasattr(func, '__call__'):  # a callable class instance
            job._instance = func
            job._func_name = '__call__'
        else:
            raise TypeError('Expected a callable or a string, but got: {0}'.format(func))
        job._args = args
        job._kwargs = kwargs

        if on_success:
            if not inspect.isfunction(on_success) and not inspect.isbuiltin(on_success):
                raise ValueError('on_success callback must be a function')
            job._success_callback_name = '{0}.{1}'.format(on_success.__module__, on_success.__qualname__)

        if on_failure:
            if not inspect.isfunction(on_failure) and not inspect.isbuiltin(on_failure):
                raise ValueError('on_failure callback must be a function')
            job._failure_callback_name = '{0}.{1}'.format(on_failure.__module__, on_failure.__qualname__)

        # Extra meta data
        job.description = description or job.get_call_string()
        job.result_ttl = parse_timeout(result_ttl)
        job.failure_ttl = parse_timeout(failure_ttl)
        job.ttl = parse_timeout(ttl)
        job.timeout = parse_timeout(timeout)
        job._status = status
        job.meta = meta or {}

        # dependency could be job instance or id, or iterable thereof
        if depends_on is not None:
            if isinstance(depends_on, Dependency):
                job.allow_dependency_failures = depends_on.allow_failure
                depends_on_list = depends_on.dependencies
            else:
                depends_on_list = ensure_list(depends_on)
            job._dependency_ids = [
                dep.id if isinstance(dep, Job) else dep
                for dep in depends_on_list
            ]

        return job

    def get_position(self):
        from .queue import Queue
        if self.origin:
            q = Queue(name=self.origin, connection=self.connection)
            return q.get_job_position(self._id)
        return None

    def get_status(self, refresh=True):
        if refresh:
            self._status = as_text(self.connection.hget(self.key, 'status'))

        return self._status

    def set_status(self, status, pipeline=None):
        self._status = status
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'status', self._status)

    def get_meta(self, refresh=True):
        if refresh:
            meta = self.connection.hget(self.key, 'meta')
            self.meta = self.serializer.loads(meta) if meta else {}

        return self.meta

    @property
    def is_finished(self):
        return self.get_status() == JobStatus.FINISHED

    @property
    def is_queued(self):
        return self.get_status() == JobStatus.QUEUED

    @property
    def is_failed(self):
        return self.get_status() == JobStatus.FAILED

    @property
    def is_started(self):
        return self.get_status() == JobStatus.STARTED

    @property
    def is_deferred(self):
        return self.get_status() == JobStatus.DEFERRED

    @property
    def is_canceled(self):
        return self.get_status() == JobStatus.CANCELED

    @property
    def is_scheduled(self):
        return self.get_status() == JobStatus.SCHEDULED

    @property
    def is_stopped(self):
        return self.get_status() == JobStatus.STOPPED

    @property
    def _dependency_id(self):
        """Returns the first item in self._dependency_ids. Present to
        preserve compatibility with third party packages..
        """
        if self._dependency_ids:
            return self._dependency_ids[0]

    @property
    def dependency(self):
        """Returns a job's first dependency. To avoid repeated Redis fetches, we cache
        job.dependency as job._dependency.
        """
        if not self._dependency_ids:
            return None
        if hasattr(self, '_dependency'):
            return self._dependency
        job = self.fetch(self._dependency_ids[0], connection=self.connection, serializer=self.serializer)
        self._dependency = job
        return job

    @property
    def dependent_ids(self):
        """Returns a list of ids of jobs whose execution depends on this
        job's successful execution."""
        return list(map(as_text, self.connection.smembers(self.dependents_key)))

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name)

        return import_attribute(self.func_name)

    @property
    def success_callback(self):
        if self._success_callback is UNEVALUATED:
            if self._success_callback_name:
                self._success_callback = import_attribute(self._success_callback_name)
            else:
                self._success_callback = None

        return self._success_callback

    @property
    def failure_callback(self):
        if self._failure_callback is UNEVALUATED:
            if self._failure_callback_name:
                self._failure_callback = import_attribute(self._failure_callback_name)
            else:
                self._failure_callback = None

        return self._failure_callback

    def _deserialize_data(self):
        try:
            self._func_name, self._instance, self._args, self._kwargs = self.serializer.loads(self.data)
        except Exception as e:
            # catch anything because serializers are generic
            raise DeserializationError() from e

    @property
    def data(self):
        if self._data is UNEVALUATED:
            if self._func_name is UNEVALUATED:
                raise ValueError('Cannot build the job data')

            if self._instance is UNEVALUATED:
                self._instance = None

            if self._args is UNEVALUATED:
                self._args = ()

            if self._kwargs is UNEVALUATED:
                self._kwargs = {}

            job_tuple = self._func_name, self._instance, self._args, self._kwargs
            self._data = self.serializer.dumps(job_tuple)
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED

    @property
    def func_name(self):
        if self._func_name is UNEVALUATED:
            self._deserialize_data()
        return self._func_name

    @func_name.setter
    def func_name(self, value):
        self._func_name = value
        self._data = UNEVALUATED

    @property
    def instance(self):
        if self._instance is UNEVALUATED:
            self._deserialize_data()
        return self._instance

    @instance.setter
    def instance(self, value):
        self._instance = value
        self._data = UNEVALUATED

    @property
    def args(self):
        if self._args is UNEVALUATED:
            self._deserialize_data()
        return self._args

    @args.setter
    def args(self, value):
        self._args = value
        self._data = UNEVALUATED

    @property
    def kwargs(self):
        if self._kwargs is UNEVALUATED:
            self._deserialize_data()
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = value
        self._data = UNEVALUATED

    @classmethod
    def exists(cls, job_id, connection=None):
        """Returns whether a job hash exists for the given job ID."""
        conn = resolve_connection(connection)
        return conn.exists(cls.key_for(job_id))

    @classmethod
    def fetch(cls, id, connection=None, serializer=None):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = cls(id, connection=connection, serializer=serializer)
        job.refresh()
        return job

    @classmethod
    def fetch_many(cls, job_ids, connection, serializer=None):
        """
        Bulk version of Job.fetch

        For any job_ids which a job does not exist, the corresponding item in
        the returned list will be None.
        """
        with connection.pipeline() as pipeline:
            for job_id in job_ids:
                pipeline.hgetall(cls.key_for(job_id))
            results = pipeline.execute()

        jobs = []
        for i, job_id in enumerate(job_ids):
            if results[i]:
                job = cls(job_id, connection=connection, serializer=serializer)
                job.restore(results[i])
                jobs.append(job)
            else:
                jobs.append(None)

        return jobs

    def __init__(self, id=None, connection=None, serializer=None):
        self.connection = resolve_connection(connection)
        self._id = id
        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self._success_callback_name = None
        self._success_callback = UNEVALUATED
        self._failure_callback_name = None
        self._failure_callback = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self.failure_ttl = None
        self.ttl = None
        self.worker_name = None
        self._status = None
        self._dependency_ids = []
        self.meta = {}
        self.serializer = resolve_serializer(serializer)
        self.retries_left = None
        # retry_intervals is a list of int e.g [60, 120, 240]
        self.retry_intervals = None
        self.redis_server_version = None
        self.last_heartbeat = None
        self.allow_dependency_failures = None

    def __repr__(self):  # noqa  # pragma: no cover
        return '{0}({1!r}, enqueued_at={2!r})'.format(self.__class__.__name__,
                                                      self._id,
                                                      self.enqueued_at)

    def __str__(self):
        return '<{0} {1}: {2}>'.format(self.__class__.__name__,
                                       self.id,
                                       self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):  # pragma: no cover
        return hash(self.id)

    # Data access
    def get_id(self):  # noqa
        """The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.
        """
        if self._id is None:
            self._id = str(uuid4())
        return self._id

    def set_id(self, value):
        """Sets a job ID for the given job."""
        if not isinstance(value, string_types):
            raise TypeError('id must be a string, not {0}'.format(type(value)))
        self._id = value

    def heartbeat(self, timestamp, ttl, pipeline=None, xx=False):
        self.last_heartbeat = timestamp
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, 'last_heartbeat', utcformat(self.last_heartbeat))
        self.started_job_registry.add(self, ttl, pipeline=pipeline, xx=xx)

    id = property(get_id, set_id)

    @classmethod
    def key_for(cls, job_id):
        """The Redis key that is used to store job hash under."""
        return (cls.redis_job_namespace_prefix + job_id).encode('utf-8')

    @classmethod
    def dependents_key_for(cls, job_id):
        """The Redis key that is used to store job dependents hash under."""
        return '{0}{1}:dependents'.format(cls.redis_job_namespace_prefix, job_id)

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return self.key_for(self.id)

    @property
    def dependents_key(self):
        """The Redis key that is used to store job dependents hash under."""
        return self.dependents_key_for(self.id)

    @property
    def dependencies_key(self):
        return '{0}:{1}:dependencies'.format(self.redis_job_namespace_prefix, self.id)

    def fetch_dependencies(self, watch=False, pipeline=None):
        """
        Fetch all of a job's dependencies. If a pipeline is supplied, and
        watch is true, then set WATCH on all the keys of all dependencies.

        Returned jobs will use self's connection, not the pipeline supplied.

        If a job has been deleted from redis, it is not returned.
        """
        connection = pipeline if pipeline is not None else self.connection

        if watch and self._dependency_ids:
            connection.watch(*[self.key_for(dependency_id)
                               for dependency_id in self._dependency_ids])

        jobs = [job
                for job in self.fetch_many(self._dependency_ids, connection=self.connection, serializer=self.serializer)
                if job]

        return jobs

    @property
    def result(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be
        None.  But when the job has been executed, and had a return value or
        exception, this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_
        been executed when its return value is None, since return values
        written back to Redis will expire after a given amount of time (500
        seconds by default).
        """
        if self._result is None:
            rv = self.connection.hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._result = self.serializer.loads(rv)
        return self._result

    """Backwards-compatibility accessor property `return_value`."""
    return_value = result

    def restore(self, raw_data):
        """Overwrite properties with the provided values stored in Redis"""
        obj = decode_redis_hash(raw_data)
        try:
            raw_data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        try:
            self.data = zlib.decompress(raw_data)
        except zlib.error:
            # Fallback to uncompressed string
            self.data = raw_data

        self.created_at = str_to_date(obj.get('created_at'))
        self.origin = as_text(obj.get('origin'))
        self.worker_name = obj.get('worker_name').decode() if obj.get('worker_name') else None
        self.description = as_text(obj.get('description'))
        self.enqueued_at = str_to_date(obj.get('enqueued_at'))
        self.started_at = str_to_date(obj.get('started_at'))
        self.ended_at = str_to_date(obj.get('ended_at'))
        self.last_heartbeat = str_to_date(obj.get('last_heartbeat'))
        result = obj.get('result')
        if result:
            try:
                self._result = self.serializer.loads(obj.get('result'))
            except Exception:
                self._result = "Unserializable return value"
        self.timeout = parse_timeout(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None
        self.failure_ttl = int(obj.get('failure_ttl')) if obj.get('failure_ttl') else None
        self._status = obj.get('status').decode() if obj.get('status') else None

        if obj.get('success_callback_name'):
            self._success_callback_name = obj.get('success_callback_name').decode()

        if obj.get('failure_callback_name'):
            self._failure_callback_name = obj.get('failure_callback_name').decode()

        dep_ids = obj.get('dependency_ids')
        dep_id = obj.get('dependency_id')  # for backwards compatibility
        self._dependency_ids = (json.loads(dep_ids.decode()) if dep_ids
                                else [dep_id.decode()] if dep_id else [])
        self.allow_dependency_failures = bool(int(obj.get('allow_dependency_failures'))) if obj.get('allow_dependency_failures') else None
        self.ttl = int(obj.get('ttl')) if obj.get('ttl') else None
        self.meta = self.serializer.loads(obj.get('meta')) if obj.get('meta') else {}

        self.retries_left = int(obj.get('retries_left')) if obj.get('retries_left') else None
        if obj.get('retry_intervals'):
            self.retry_intervals = json.loads(obj.get('retry_intervals').decode())

        raw_exc_info = obj.get('exc_info')
        if raw_exc_info:
            try:
                self.exc_info = as_text(zlib.decompress(raw_exc_info))
            except zlib.error:
                # Fallback to uncompressed string
                self.exc_info = as_text(raw_exc_info)

    # Persistence
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        data = self.connection.hgetall(self.key)
        if not data:
            raise NoSuchJobError('No such job: {0}'.format(self.key))
        self.restore(data)

    def to_dict(self, include_meta=True):
        """
        Returns a serialization of the current job instance

        You can exclude serializing the `meta` dictionary by setting
        `include_meta=False`.
        """
        obj = {
            'created_at': utcformat(self.created_at or utcnow()),
            'data': zlib.compress(self.data),
            'success_callback_name': self._success_callback_name if self._success_callback_name else '',
            'failure_callback_name': self._failure_callback_name if self._failure_callback_name else '',
            'started_at': utcformat(self.started_at) if self.started_at else '',
            'ended_at': utcformat(self.ended_at) if self.ended_at else '',
            'last_heartbeat': utcformat(self.last_heartbeat) if self.last_heartbeat else '',
            'worker_name': self.worker_name or ''
        }

        if self.retries_left is not None:
            obj['retries_left'] = self.retries_left
        if self.retry_intervals is not None:
            obj['retry_intervals'] = json.dumps(self.retry_intervals)
        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = utcformat(self.enqueued_at)

        if self._result is not None:
            try:
                obj['result'] = self.serializer.dumps(self._result)
            except:  # noqa
                obj['result'] = "Unserializable return value"
        if self.exc_info is not None:
            obj['exc_info'] = zlib.compress(str(self.exc_info).encode('utf-8'))
        if self.timeout is not None:
            obj['timeout'] = self.timeout
        if self.result_ttl is not None:
            obj['result_ttl'] = self.result_ttl
        if self.failure_ttl is not None:
            obj['failure_ttl'] = self.failure_ttl
        if self._status is not None:
            obj['status'] = self._status
        if self._dependency_ids:
            obj['dependency_id'] = self._dependency_ids[0]  # for backwards compatibility
            obj['dependency_ids'] = json.dumps(self._dependency_ids)
        if self.meta and include_meta:
            obj['meta'] = self.serializer.dumps(self.meta)
        if self.ttl:
            obj['ttl'] = self.ttl

        if self.allow_dependency_failures is not None:
            # convert boolean to integer to avoid redis.exception.DataError
            obj["allow_dependency_failures"] = int(self.allow_dependency_failures)

        return obj

    def save(self, pipeline=None, include_meta=True):
        """
        Dumps the current job instance to its corresponding Redis key.

        Exclude saving the `meta` dictionary by setting
        `include_meta=False`. This is useful to prevent clobbering
        user metadata without an expensive `refresh()` call first.

        Redis key persistence may be altered by `cleanup()` method.
        """
        key = self.key
        connection = pipeline if pipeline is not None else self.connection

        mapping = self.to_dict(include_meta=include_meta)

        if self.get_redis_server_version() >= (4, 0, 0):
            connection.hset(key, mapping=mapping)
        else:
            connection.hmset(key, mapping)

    def get_redis_server_version(self):
        """Return Redis server version of connection"""
        if not self.redis_server_version:
            self.redis_server_version = get_version(self.connection)

        return self.redis_server_version

    def save_meta(self):
        """Stores job meta from the job instance to the corresponding Redis key."""
        meta = self.serializer.dumps(self.meta)
        self.connection.hset(self.key, 'meta', meta)

    def cancel(self, pipeline=None, enqueue_dependents=False):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.

        You can enqueue the jobs dependents optionally, 
        Same pipelining behavior as Queue.enqueue_dependents on whether or not a pipeline is passed in.
        """
        if self.is_canceled:
            raise InvalidJobOperation("Cannot cancel already canceled job: {}".format(self.get_id()))
        from .registry import CanceledJobRegistry
        from .queue import Queue
        pipe = pipeline or self.connection.pipeline()

        while True:
            try:
                q = Queue(
                    name=self.origin,
                    connection=self.connection,
                    job_class=self.__class__,
                    serializer=self.serializer
                )

                self.set_status(JobStatus.CANCELED, pipeline=pipe)
                if enqueue_dependents:
                    # Only WATCH if no pipeline passed, otherwise caller is responsible
                    if pipeline is None:
                        pipe.watch(self.dependents_key)
                    q.enqueue_dependents(self, pipeline=pipeline)
                self._remove_from_registries(
                    pipeline=pipe,
                    remove_from_queue=True
                )

                registry = CanceledJobRegistry(
                    self.origin,
                    self.connection,
                    job_class=self.__class__,
                    serializer=self.serializer
                )
                registry.add(self, pipeline=pipe)
                if pipeline is None:
                    pipe.execute()
                break
            except WatchError:
                if pipeline is None:
                    continue
                else:
                    # if the pipeline comes from the caller, we re-raise the
                    # exception as it is the responsibility of the caller to
                    # handle it
                    raise

    def requeue(self, at_front=False):
        """Requeues job."""
        return self.failed_job_registry.requeue(self, at_front=at_front)

    def _remove_from_registries(self, pipeline=None, remove_from_queue=True):
        if remove_from_queue:
            from .queue import Queue
            q = Queue(name=self.origin, connection=self.connection, serializer=self.serializer)
            q.remove(self, pipeline=pipeline)

        if self.is_finished:
            from .registry import FinishedJobRegistry
            registry = FinishedJobRegistry(self.origin,
                                           connection=self.connection,
                                           job_class=self.__class__,
                                           serializer=self.serializer)
            registry.remove(self, pipeline=pipeline)

        elif self.is_deferred:
            from .registry import DeferredJobRegistry
            registry = DeferredJobRegistry(self.origin,
                                           connection=self.connection,
                                           job_class=self.__class__,
                                           serializer=self.serializer)
            registry.remove(self, pipeline=pipeline)

        elif self.is_started:
            from .registry import StartedJobRegistry
            registry = StartedJobRegistry(self.origin,
                                          connection=self.connection,
                                          job_class=self.__class__,
                                          serializer=self.serializer)
            registry.remove(self, pipeline=pipeline)

        elif self.is_scheduled:
            from .registry import ScheduledJobRegistry
            registry = ScheduledJobRegistry(self.origin,
                                            connection=self.connection,
                                            job_class=self.__class__,
                                            serializer=self.serializer)
            registry.remove(self, pipeline=pipeline)

        elif self.is_failed or self.is_stopped:
            self.failed_job_registry.remove(self, pipeline=pipeline)

        elif self.is_canceled:
            from .registry import CanceledJobRegistry
            registry = CanceledJobRegistry(self.origin, connection=self.connection,
                                           job_class=self.__class__,
                                           serializer=self.serializer)
            registry.remove(self, pipeline=pipeline)

    def delete(self, pipeline=None, remove_from_queue=True,
               delete_dependents=False):
        """Cancels the job and deletes the job hash from Redis. Jobs depending
        on this job can optionally be deleted as well."""

        connection = pipeline if pipeline is not None else self.connection

        self._remove_from_registries(pipeline=pipeline, remove_from_queue=remove_from_queue)

        if delete_dependents:
            self.delete_dependents(pipeline=pipeline)

        connection.delete(self.key, self.dependents_key, self.dependencies_key)

    def delete_dependents(self, pipeline=None):
        """Delete jobs depending on this job."""
        connection = pipeline if pipeline is not None else self.connection
        for dependent_id in self.dependent_ids:
            try:
                job = Job.fetch(dependent_id, connection=self.connection, serializer=self.serializer)
                job.delete(pipeline=pipeline,
                           remove_from_queue=False)
            except NoSuchJobError:
                # It could be that the dependent job was never saved to redis
                pass
        connection.delete(self.dependents_key)

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments."""
        self.connection.persist(self.key)
        _job_stack.push(self)
        try:
            self._result = self._execute()
        finally:
            assert self is _job_stack.pop()
        return self._result

    def prepare_for_execution(self, worker_name, pipeline):
        """Set job metadata before execution begins"""
        self.worker_name = worker_name
        self.last_heartbeat = utcnow()
        self.started_at = self.last_heartbeat
        self._status = JobStatus.STARTED
        mapping = {
            'last_heartbeat': utcformat(self.last_heartbeat),
            'status': self._status,
            'started_at': utcformat(self.started_at),
            'worker_name': worker_name
        }
        if self.get_redis_server_version() >= (4, 0, 0):
            pipeline.hset(self.key, mapping=mapping)
        else:
            pipeline.hmset(self.key, mapping)

    def _execute(self):
        result = self.func(*self.args, **self.kwargs)
        if asyncio.iscoroutine(result):
            loop = asyncio.new_event_loop()
            coro_result = loop.run_until_complete(result)
            return coro_result
        return result

    def get_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a job will be
        persisted. In the future, this method will also be responsible
        for determining ttl for repeated jobs.
        """
        return default_ttl if self.ttl is None else self.ttl

    def get_result_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a jobs result will
        be persisted. In the future, this method will also be responsible
        for determining ttl for repeated jobs.
        """
        return default_ttl if self.result_ttl is None else self.result_ttl

    # Representation
    def get_call_string(self):  # noqa
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement.
        """
        return get_call_string(self.func_name, self.args, self.kwargs, max_length=75)

    def cleanup(self, ttl=None, pipeline=None, remove_from_queue=True):
        """Prepare job for eventual deletion (if needed). This method is usually
        called after successful execution. How long we persist the job and its
        result depends on the value of ttl:
        - If ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If ttl is negative, don't set an expiry to it (persist
          forever)
        """
        if ttl == 0:
            self.delete(pipeline=pipeline, remove_from_queue=remove_from_queue)
        elif not ttl:
            return
        elif ttl > 0:
            connection = pipeline if pipeline is not None else self.connection
            connection.expire(self.key, ttl)
            connection.expire(self.dependents_key, ttl)
            connection.expire(self.dependencies_key, ttl)

    @property
    def started_job_registry(self):
        from .registry import StartedJobRegistry
        return StartedJobRegistry(self.origin, connection=self.connection,
                                  job_class=self.__class__,
                                  serializer=self.serializer)

    @property
    def failed_job_registry(self):
        from .registry import FailedJobRegistry
        return FailedJobRegistry(self.origin, connection=self.connection,
                                 job_class=self.__class__,
                                 serializer=self.serializer)

    def get_retry_interval(self):
        """Returns the desired retry interval.
        If number of retries is bigger than length of intervals, the first
        value in the list will be used multiple times.
        """
        if self.retry_intervals is None:
            return 0
        number_of_intervals = len(self.retry_intervals)
        index = max(number_of_intervals - self.retries_left, 0)
        return self.retry_intervals[index]

    def retry(self, queue, pipeline):
        """Requeue or schedule this job for execution"""
        retry_interval = self.get_retry_interval()
        self.retries_left = self.retries_left - 1
        if retry_interval:
            scheduled_datetime = datetime.now(timezone.utc) + timedelta(seconds=retry_interval)
            self.set_status(JobStatus.SCHEDULED)
            queue.schedule_job(self, scheduled_datetime, pipeline=pipeline)
        else:
            queue.enqueue_job(self, pipeline=pipeline)

    def register_dependency(self, pipeline=None):
        """Jobs may have dependencies. Jobs are enqueued only if the jobs they
        depend on are successfully performed. We record this relation as
        a reverse dependency (a Redis set), with a key that looks something
        like:

            rq:job:job_id:dependents = {'job_id_1', 'job_id_2'}

        This method adds the job in its dependencies' dependents sets,
        and adds the job to DeferredJobRegistry.
        """
        from .registry import DeferredJobRegistry

        registry = DeferredJobRegistry(self.origin,
                                       connection=self.connection,
                                       job_class=self.__class__,
                                       serializer=self.serializer)
        registry.add(self, pipeline=pipeline)

        connection = pipeline if pipeline is not None else self.connection

        for dependency_id in self._dependency_ids:
            dependents_key = self.dependents_key_for(dependency_id)
            connection.sadd(dependents_key, self.id)
            connection.sadd(self.dependencies_key, dependency_id)

    @property
    def dependency_ids(self):
        dependencies = self.connection.smembers(self.dependencies_key)
        return [Job.key_for(_id.decode())
                for _id in dependencies]

    def dependencies_are_met(self, parent_job=None, pipeline=None):
        """Returns a boolean indicating if all of this job's dependencies are _FINISHED_

        If a pipeline is passed, all dependencies are WATCHed.

        `parent_job` allows us to directly pass parent_job for the status check.
        This is useful when enqueueing the dependents of a _successful_ job -- that status of
        `FINISHED` may not be yet set in redis, but said job is indeed _done_ and this
        method is _called_ in the _stack_ of its dependents are being enqueued.
        """
        connection = pipeline if pipeline is not None else self.connection

        if pipeline is not None:
            connection.watch(*[self.key_for(dependency_id)
                               for dependency_id in self._dependency_ids])

        dependencies_ids = {_id.decode()
                            for _id in connection.smembers(self.dependencies_key)}

        if parent_job:
            # If parent job is canceled, no need to check for status
            # If parent job is not finished, we should only continue
            # if this job allows parent job to fail
            dependencies_ids.discard(parent_job.id)
            if parent_job._status == JobStatus.CANCELED:
                pass
            elif parent_job._status == JobStatus.FAILED and not self.allow_dependency_failures:
                return False

            # If the only dependency is parent job, dependency has been met
            if not dependencies_ids:
                return True

        with connection.pipeline() as pipeline:
            for key in dependencies_ids:
                pipeline.hget(self.key_for(key), 'status')

            dependencies_statuses = pipeline.execute()

        if self.allow_dependency_failures:
            allowed_statuses = [JobStatus.FINISHED, JobStatus.FAILED]
        else:
            allowed_statuses = [JobStatus.FINISHED]

        return all(
            status.decode() in allowed_statuses
            for status
            in dependencies_statuses
            if status
        )


_job_stack = LocalStack()


class Retry:
    def __init__(self, max, interval=0):
        """`interval` can be a positive number or a list of ints"""
        super().__init__()
        if max < 1:
            raise ValueError('max: please enter a value greater than 0')

        if isinstance(interval, int):
            if interval < 0:
                raise ValueError('interval: negative numbers are not allowed')
            intervals = [interval]
        elif isinstance(interval, Iterable):
            for i in interval:
                if i < 0:
                    raise ValueError('interval: negative numbers are not allowed')
            intervals = interval

        self.max = max
        self.intervals = intervals
