# -*- coding: utf-8 -*-
# Copyright 2012 Vincent Driessen. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY VINCENT DRIESSEN ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL VINCENT DRIESSEN OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are those
# of the authors and should not be interpreted as representing official policies,
# either expressed or implied, of Vincent Driessen.

from __future__ import absolute_import, division, print_function, unicode_literals

import inspect
import json
import logging
import warnings
import zlib

from collections.abc import Iterable
from distutils.version import StrictVersion
from enum import Enum
from uuid import uuid4

from redis import WatchError, ResponseError

from rq.compat import as_text, decode_redis_hash, string_types

from .connections import resolve_connection
from .exceptions import NoSuchJobError
from .local import LocalStack
from .serializers import resolve_serializer
from .utils import import_attribute, parse_timeout, str_to_date, utcformat, utcnow, enum

# Serialize pickle dumps using the highest pickle protocol (binary, default
# uses ascii)
logger = logging.getLogger(__name__)


JobStatus = enum(
    "JobStatus",
    QUEUED="queued",
    FINISHED="finished",
    FAILED="failed",
    STARTED="started",
    DEFERRED="deferred",
    CANCELLED="cancelled",
    SCHEDULED="scheduled",
)

setattr(JobStatus, "values", classmethod(lambda cls: cls._values.values()))
setattr(JobStatus, "keys", classmethod(lambda cls: cls._values.keys()))
setattr(JobStatus, "items", classmethod(lambda cls: cls._values.items()))
setattr(JobStatus, "terminal", classmethod(lambda cls, value: value in (cls.FINISHED, cls.FAILED, cls.CANCELLED)))

JobMetadataKeys = enum("JobMetadataKeys", DEPENDENCY_STATUS="dependency_status", CANCELLED="cancelled",)

JobRunConditionStatus = enum(
    "JobRunConditionStatus",
    CANNOT_RUN="cannot_run",
    CAN_RUN="can_run",
    DEPENDENCIES_UNFINISHED="dependencies_unfinished",
)


class RunCondition(Enum):
    """
    Run Conditions define when to run a job if upstream has failed, cancelled or succeeded.
    """

    FAILURE = "on_failure"
    SUCCESS = "on_success"

    @classmethod
    def can_run(cls, conditions, upstream_status) -> bool:
        """
        Checks if provided job's run conditions allows this particular job to run given its parent job status.

        :param conditions:        Job's conditions to run.
        :param upstream_status:   Parent job's execution status.
        :return:                  True if run conditions and the upstream status allow a job to run.
        """
        if isinstance(upstream_status, JobStatus):
            upstream_status = upstream_status
        if upstream_status in (
            JobStatus.CANCELLED,
            JobStatus.QUEUED,
            JobStatus.DEFERRED,
            JobStatus.SCHEDULED,
            JobStatus.STARTED,
        ):
            return False
        if upstream_status == JobStatus.FINISHED:
            return cls.SUCCESS in conditions
        if upstream_status == JobStatus.FAILED:
            return cls.FAILURE in conditions
        raise ValueError("Unknown upstream status: {}".format(upstream_status))


# Sentinel value to mark that some of our lazily evaluated properties have not
# yet been evaluated.
UNEVALUATED = object()


def truncate_long_string(data, maxlen=75):
    """ Truncates strings longer than maxlen
    """
    return (data[:maxlen] + "...") if len(data) > maxlen else data


def cancel_job(job_id, connection=None):
    """Cancels the job with the given job ID, preventing execution.  Discards
    any job info (i.e. it can't be requeued later).
    """
    Job.fetch(job_id, connection=connection).cancel()


def get_current_job(connection=None, job_class=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    if job_class:
        warnings.warn("job_class argument for get_current_job is deprecated.", DeprecationWarning)
    return _job_stack.top


def requeue_job(job_id, connection):
    job = Job.fetch(job_id, connection=connection)
    return job.requeue()


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """

    redis_job_namespace_prefix = "rq:job:"

    # Job construction
    @classmethod
    def create(
        cls,
        func,
        args=None,
        kwargs=None,
        connection=None,
        result_ttl=None,
        ttl=None,
        status=None,
        description=None,
        depends_on=None,
        timeout=None,
        id=None,
        origin=None,
        meta=None,
        failure_ttl=None,
        serializer=None,
    ):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError("{0!r} is not a valid args list".format(args))
        if not isinstance(kwargs, dict):
            raise TypeError("{0!r} is not a valid kwargs dict".format(kwargs))

        # Validate run conditions and callbacks
        on_failure = (meta or {}).pop("on_failure", False)
        on_success = (meta or {}).pop("on_success", False)
        run_when = (meta or {}).pop("run_when", [RunCondition.SUCCESS])
        if isinstance(run_when, RunCondition):
            run_when = [run_when]
        elif isinstance(run_when, Iterable):  # pylint: disable=isinstance-second-argument-not-valid-type
            run_when = list(run_when)
        else:
            raise ValueError(
                "Metadata 'run_when' must be an iterable of {0!r}"
                "or a single {0!r}, but is {1!r}".format(RunCondition.__name__, type(run_when))
            )
        if not run_when:
            raise ValueError("Metadata 'run_when' list of run conditions cannot be empty!")

        meta = {
            "on_failure": on_failure,
            "on_success": on_success,
            "run_when": run_when,
            **(meta or {}),
        }

        job = cls(connection=connection, serializer=serializer)
        if id is not None:
            job.id = id

        if origin is not None:
            job.origin = origin

        # Set the core job tuple properties
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = "{0}.{1}".format(func.__module__, func.__name__)
        elif isinstance(func, string_types):
            job._func_name = as_text(func)
        elif not inspect.isclass(func) and hasattr(func, "__call__"):  # a callable class instance
            job._instance = func
            job._func_name = "__call__"
        else:
            raise TypeError("Expected a callable or a string, but got: {0}".format(func))
        job._args = args
        job._kwargs = kwargs

        # Extra meta data
        job.description = description or job.get_call_string()
        job.result_ttl = parse_timeout(result_ttl)
        job.failure_ttl = parse_timeout(failure_ttl)
        job.ttl = parse_timeout(ttl)
        job.timeout = parse_timeout(timeout)
        job._status = status
        job.meta = meta or {}

        # dependency could be job instance or id
        if depends_on is not None:
            job._dependency_ids = [depends_on.id if isinstance(depends_on, Job) else depends_on]
        return job

    def get_position(self):
        from .queue import Queue

        if self.origin:
            q = Queue(name=self.origin, connection=self.connection)
            return q.get_job_position(self._id)
        return None

    def get_status(self, refresh=True):
        if refresh:
            self._status = as_text(self.connection.hget(self.key, "status"))

        return self._status

    def set_status(self, status, pipeline=None):
        self._status = status
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, "status", self._status)

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
    def is_scheduled(self):
        return self.get_status() == JobStatus.SCHEDULED

    @property
    def _dependency_id(self):
        """Returns the first item in self._dependency_ids. Present
        preserve compatibility with third party packages..
        """
        if self._dependency_ids:
            return self._dependency_ids[0]

    @property
    def dependency(self):
        """Returns a job's dependency. To avoid repeated Redis fetches, we cache
        job.dependency as job._dependency.
        """
        if not self._dependency_ids:
            return None
        if hasattr(self, "_dependency"):
            return self._dependency
        job = self.fetch(self._dependency_ids[0], connection=self.connection)
        self._dependency = job
        return job

    @property
    def dependent_ids(self):
        """
        Returns a list of ids of jobs whose execution depends on this
        job's execution.
        Note that dependent jobs may run even when this job fails, depending on `run_when` property.
        """
        return list(map(as_text, self.connection.smembers(self.dependents_key)))

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name)

        return import_attribute(self.func_name)

    def _deserialize_data(self):
        self._func_name, self._instance, self._args, self._kwargs = self.serializer.loads(self.data)

    @property
    def data(self):
        if self._data is UNEVALUATED:
            if self._func_name is UNEVALUATED:
                raise ValueError("Cannot build the job data")

            if self._instance is UNEVALUATED:
                self._instance = None

            if self._args is UNEVALUATED:
                self._args = ()

            if self._kwargs is UNEVALUATED:
                self._kwargs = {}

            job_tuple = self._func_name, self._instance, self._args, self._kwargs
            self._data = self.serializer.dumps(job_tuple)
        return self._data

    # noinspection PyUnresolvedReferences,PyPropertyDefinition
    @data.setter
    def data(self, value):
        self._data = value
        # noinspection PyTypeChecker
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        # noinspection PyTypeChecker
        self._args = UNEVALUATED
        # noinspection PyTypeChecker
        self._kwargs = UNEVALUATED

    @property
    def func_name(self):
        if self._func_name is UNEVALUATED:
            self._deserialize_data()
        return self._func_name

    # noinspection PyUnresolvedReferences,PyPropertyDefinition
    @func_name.setter
    def func_name(self, value):
        self._func_name = value
        self._data = UNEVALUATED

    @property
    def instance(self):
        if self._instance is UNEVALUATED:
            self._deserialize_data()
        return self._instance

    # noinspection PyUnresolvedReferences,PyPropertyDefinition
    @instance.setter
    def instance(self, value):
        self._instance = value
        self._data = UNEVALUATED

    @property
    def args(self):
        if self._args is UNEVALUATED:
            self._deserialize_data()
        return self._args

    # noinspection PyUnresolvedReferences,PyPropertyDefinition
    @args.setter
    def args(self, value):
        self._args = value
        self._data = UNEVALUATED

    @property
    def kwargs(self):
        if self._kwargs is UNEVALUATED:
            self._deserialize_data()
        return self._kwargs

    # noinspection PyUnresolvedReferences,PyPropertyDefinition
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
    def fetch_many(cls, job_ids, connection):
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
                job = cls(job_id, connection=connection)
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
        self._func_name = UNEVALUATED  # noqa
        self._instance = UNEVALUATED
        self._args = UNEVALUATED  # noqa
        self._kwargs = UNEVALUATED  # noqa
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
        self._status = None
        self._dependency_ids = []
        self.meta = {}
        self.serializer = resolve_serializer(serializer)
        self.retries_left = None
        # retry_intervals is a list of int e.g [60, 120, 240]
        self.retry_intervals = None
        self.redis_server_version = None

    def __repr__(self):  # noqa  # pragma: no cover
        return "{0}({1!r}, enqueued_at={2!r})".format(self.__class__.__name__, self._id, self.enqueued_at)

    def __str__(self):
        return "<{0} {1}: {2}>".format(self.__class__.__name__, self.id, self.description)

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
            raise TypeError("id must be a string, not {0}".format(type(value)))
        self._id = value

    id = property(get_id, set_id)

    @classmethod
    def key_for(cls, job_id):
        """The Redis key that is used to store job hash under."""
        return (cls.redis_job_namespace_prefix + job_id).encode("utf-8")

    @classmethod
    def dependents_key_for(cls, job_id):
        """The Redis key that is used to store job dependents hash under."""
        return "{0}{1}:dependents".format(cls.redis_job_namespace_prefix, job_id)

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
        return "{0}:{1}:dependencies".format(self.redis_job_namespace_prefix, self.id)

    def fetch_dependencies(self, watch=False, pipeline=None):
        """
        Fetch all of a job's dependencies. If a pipeline is supplied, and
        watch is true, then set WATCH on all the keys of all dependencies.

        Returned jobs will use self's connection, not the pipeline supplied.

        If a job has been deleted from redis, it is not returned.
        """
        connection = pipeline if pipeline is not None else self.connection

        if watch and self._dependency_ids:
            connection.watch(*self._dependency_ids)

        jobs = [job for job in self.fetch_many(self._dependency_ids, connection=self.connection) if job]

        return jobs

    @classmethod
    def _get_linked_jobs(
        cls, job: "Job", link_key_getter, recursive, pipeline, raise_on_no_such_job: bool = True,
    ):
        """
        Get linked jobs from a given job using link key.
        This method is intended to be used in get_parents() / get_children()

        :param link_key_getter:  Dependency key or dependents key.
        :param pipeline:  Command pipeline.
        :param recursive: If True, recursively get all dependent jobs of this job.
        :return:          A list of linked job ids at the moment.
        """
        pipe = pipeline if pipeline is not None else job.connection.pipeline()
        link_key = link_key_getter(job)
        result = []
        while True:
            try:
                if pipeline is None:
                    pipe.watch(link_key)
                direct_linked_job_ids = [as_text(job_id) for job_id in pipe.smembers(link_key)]
                result.extend(direct_linked_job_ids)
                if recursive:
                    for direct_linked_job_id in direct_linked_job_ids:
                        # noinspection PyProtectedMember
                        try:
                            direct_linked_job = cls.fetch(direct_linked_job_id, connection=job.connection)
                        except NoSuchJobError as e:
                            if not raise_on_no_such_job:
                                break
                            else:
                                raise e
                        distant_link_ids = direct_linked_job._get_linked_jobs(  # noqa
                            direct_linked_job, link_key_getter=link_key_getter, recursive=recursive, pipeline=pipeline,
                        )
                        result.extend(distant_link_ids)
                return result
            except WatchError:
                if pipeline is None:
                    continue
                else:
                    # if the pipeline comes from the caller, we re-raise the
                    # exception as it it the responsibility of the caller to
                    # handle it
                    raise

    def get_parent_ids(self, recursive=False, pipeline=None, raise_on_no_such_job=True):
        """
        Get dependent jobs from a given job.

        :param recursive:            If True, recursively get all dependencies for current job.
        :param raise_on_no_such_job: Controls behavior when a NoSuchJobError raised on getting dependencies.
                                     Works when recursive is set to True.
                                     When true, will raise internal NoSuchJobError.
                                     When false, will
        :param pipeline:             Command pipeline.
        :return:                     A list of all parent job IDs at the moment.
        """
        return self._get_linked_jobs(
            self,
            link_key_getter=lambda j: j.dependencies_key,
            recursive=recursive,
            pipeline=pipeline,
            raise_on_no_such_job=raise_on_no_such_job,
        )

    def get_parents(self, recursive=False, pipeline=None, raise_on_no_such_job=True):
        """
        Get dependent jobs from a given job.

        :param recursive: If True, recursively get all dependent jobs of this job.
        :param pipeline:  Command pipeline.
        :return:          A list of all parent jobs at the moment.
        """
        return [
            self.fetch(job_id, connection=self.connection)
            for job_id in self._get_linked_jobs(
                self,
                link_key_getter=lambda j: j.dependencies_key,
                recursive=recursive,
                pipeline=pipeline,
                raise_on_no_such_job=raise_on_no_such_job,
            )
        ]

    def get_children(self, recursive=False, pipeline=None, raise_on_no_such_job=True):
        """
        Get child jobs from a given job.

        :param recursive: If True, recursively get all dependent jobs of this job.
        :param pipeline:  Command pipeline.
        :return:          A list of all child jobs at the moment.
        """
        return [
            self.fetch(job_id, connection=self.connection)
            for job_id in self._get_linked_jobs(
                self,
                link_key_getter=lambda j: j.dependents_key,
                recursive=recursive,
                pipeline=pipeline,
                raise_on_no_such_job=raise_on_no_such_job,
            )
        ]

    def get_child_ids(self, recursive=False, pipeline=None, raise_on_no_such_job=True):
        """
        Get child jobs from a given job.

        :param recursive: If True, recursively get all dependent jobs of this job.
        :param pipeline:  Command pipeline.
        :return:          A list of all child job IDs at the moment.
        """
        return self._get_linked_jobs(
            self,
            link_key_getter=lambda j: j.dependents_key,
            recursive=recursive,
            pipeline=pipeline,
            raise_on_no_such_job=raise_on_no_such_job,
        )

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
            rv = self.connection.hget(self.key, "result")
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
            raw_data = obj["data"]
        except KeyError:
            raise NoSuchJobError("Unexpected job format: {0}".format(obj))

        try:
            self.data = zlib.decompress(raw_data)
        except zlib.error:
            # Fallback to uncompressed string
            self.data = raw_data

        self.created_at = str_to_date(obj.get("created_at"))
        self.origin = as_text(obj.get("origin"))
        self.description = as_text(obj.get("description"))
        self.enqueued_at = str_to_date(obj.get("enqueued_at"))
        self.started_at = str_to_date(obj.get("started_at"))
        self.ended_at = str_to_date(obj.get("ended_at"))
        result = obj.get("result")
        if result:
            try:
                self._result = self.serializer.loads(obj.get("result"))
            except Exception as e:
                self._result = "Unserializable return value"
        self.timeout = parse_timeout(obj.get("timeout")) if obj.get("timeout") else None
        self.result_ttl = int(obj.get("result_ttl")) if obj.get("result_ttl") else None  # noqa
        self.failure_ttl = int(obj.get("failure_ttl")) if obj.get("failure_ttl") else None  # noqa
        self._status = obj.get("status").decode() if obj.get("status") else None

        dependency_id = obj.get("dependency_id", None)
        self._dependency_ids = [as_text(dependency_id)] if dependency_id else []

        self.ttl = int(obj.get("ttl")) if obj.get("ttl") else None
        self.meta = self.serializer.loads(obj.get("meta")) if obj.get("meta") else {}

        self.retries_left = int(obj.get("retries_left")) if obj.get("retries_left") else None
        if obj.get("retry_intervals"):
            self.retry_intervals = json.loads(obj.get("retry_intervals").decode())

        raw_exc_info = obj.get("exc_info")
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
            raise NoSuchJobError("No such job: {0}".format(self.key))
        self.restore(data)

    def to_dict(self, include_meta=True):
        """
        Returns a serialization of the current job instance

        You can exclude serializing the `meta` dictionary by setting
        `include_meta=False`.
        """
        obj = {
            "created_at": utcformat(self.created_at or utcnow()),
            "data": zlib.compress(self.data),
            "started_at": utcformat(self.started_at) if self.started_at else "",
            "ended_at": utcformat(self.ended_at) if self.ended_at else "",
        }

        if self.retries_left is not None:
            obj["retries_left"] = self.retries_left
        if self.retry_intervals is not None:
            obj["retry_intervals"] = json.dumps(self.retry_intervals)
        if self.origin is not None:
            obj["origin"] = self.origin
        if self.description is not None:
            obj["description"] = self.description
        if self.enqueued_at is not None:
            obj["enqueued_at"] = utcformat(self.enqueued_at)

        if self._result is not None:
            try:
                obj["result"] = self.serializer.dumps(self._result)
            except Exception as e:
                obj["result"] = "Unserializable return value"
        if self.exc_info is not None:
            obj["exc_info"] = zlib.compress(str(self.exc_info).encode("utf-8"))
        if self.timeout is not None:
            obj["timeout"] = self.timeout
        if self.result_ttl is not None:
            obj["result_ttl"] = self.result_ttl
        if self.failure_ttl is not None:
            obj["failure_ttl"] = self.failure_ttl
        if self._status is not None:
            obj["status"] = self._status
        if self._dependency_ids:
            obj["dependency_id"] = self._dependency_ids[0]
        if self.meta and include_meta:
            obj["meta"] = self.serializer.dumps(self.meta)
        if self.ttl:
            obj["ttl"] = self.ttl

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

        if self.get_redis_server_version() >= StrictVersion("4.0.0"):
            connection.hset(key, mapping=mapping)
        else:
            connection.hmset(key, mapping)

    def get_redis_server_version(self, fallback=True):
        """Return Redis server version of connection"""
        if not self.redis_server_version:
            try:
                self.redis_server_version = StrictVersion(self.connection.info("server")["redis_version"])
            except ResponseError as exc:
                if fallback:
                    return StrictVersion("0.0.0")
                else:
                    raise exc from exc
        return self.redis_server_version

    def save_meta(self):
        """Stores job meta from the job instance to the corresponding Redis key."""
        meta = self.serializer.dumps(self.meta)
        self.connection.hset(self.key, "meta", meta)

    def cancel(self, pipeline=None):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """
        pipe = pipeline if pipeline is not None else self.connection.pipeline()
        dependents_key = self.dependents_key
        while True:
            try:
                pipe.watch(dependents_key)
                self.meta[JobMetadataKeys.CANCELLED] = True
                self.set_status(JobStatus.CANCELLED, pipeline=pipe)
                self.save_meta()
                if self.origin and not self.dependent_ids:
                    from .queue import Queue

                    queue = Queue(name=self.origin, connection=self.connection)
                    queue.remove(self, pipeline=pipe)
                # enqueue_dependents calls multi() on the pipeline!
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

    @property
    def run_when(self):
        return self.meta["run_when"]

    @property
    def on_failure(self):
        return self.meta.get("on_failure", None)

    @property
    def on_success(self):
        return self.meta.get("on_success", None)

    def process_dependencies(self, job_status, pipeline=None) -> None:
        pipe = pipeline or self.connection.pipeline()
        dependents_key = self.dependents_key
        while True:
            try:
                pipe.watch(dependents_key)
                for dependency in self.get_children(pipeline=pipeline, recursive=False):
                    pipe.watch(dependents_key, dependency.key_for(dependency.id))
                    if JobMetadataKeys.DEPENDENCY_STATUS not in dependency.meta:
                        dependency.meta[JobMetadataKeys.DEPENDENCY_STATUS] = {}
                    dependency.meta[JobMetadataKeys.DEPENDENCY_STATUS][self.id] = job_status
                    dependency.save_meta()
                # enqueue_dependents calls multi() on the pipeline!
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

    def check_run_condition(self, pipeline=None, refresh: bool = False):
        """
        Checks whether all dependencies
        """
        pipe = pipeline or self.connection.pipeline()
        dependencies_key = self.dependencies_key
        while True:
            try:
                pipe.watch(dependencies_key)
                if JobMetadataKeys.DEPENDENCY_STATUS not in self.meta and not refresh:
                    return JobRunConditionStatus.DEPENDENCIES_UNFINISHED
                dependency_statuses = self.meta.get(JobMetadataKeys.DEPENDENCY_STATUS, {})
                for dependency_id in self.dependency_ids:
                    pipe.unwatch()
                    pipe.watch(dependencies_key, self.key_for(dependency_id))
                    if refresh:
                        try:
                            dependency_statuses[dependency_id] = as_text(
                                self.connection.hget(self.key_for(dependency_id), "status")
                            )
                        except NoSuchJobError:
                            pass
                    if dependencies_key not in dependency_statuses:
                        result = JobRunConditionStatus.DEPENDENCIES_UNFINISHED
                        break
                    if not RunCondition.can_run(self.run_when, upstream_status=dependency_statuses[dependency_id]):
                        result = JobRunConditionStatus.CANNOT_RUN
                        break
                else:
                    result = JobRunConditionStatus.CAN_RUN
                if refresh:
                    self.meta[JobMetadataKeys.DEPENDENCY_STATUS] = dependency_statuses
                    self.save_meta()
                return result
            except WatchError:
                if pipeline is None:
                    continue
                else:
                    # if the pipeline comes from the caller, we re-raise the
                    # exception as it it the responsibility of the caller to
                    # handle it
                    raise

    def requeue(self):
        """Requeues job."""
        return self.failed_job_registry.requeue(self)

    def delete(self, pipeline=None, remove_from_queue=True, delete_dependents=False):
        """Cancels the job and deletes the job hash from Redis. Jobs depending
        on this job can optionally be deleted as well."""
        if remove_from_queue:
            self.cancel(pipeline=pipeline)
        connection = pipeline if pipeline is not None else self.connection

        if self.is_finished:
            from .registry import FinishedJobRegistry

            registry = FinishedJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)
            registry.remove(self, pipeline=pipeline)

        elif self.is_deferred:
            from .registry import DeferredJobRegistry

            registry = DeferredJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)
            registry.remove(self, pipeline=pipeline)

        elif self.is_started:
            from .registry import StartedJobRegistry

            registry = StartedJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)
            registry.remove(self, pipeline=pipeline)

        elif self.is_scheduled:
            from .registry import ScheduledJobRegistry

            registry = ScheduledJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)
            registry.remove(self, pipeline=pipeline)

        elif self.is_failed:
            self.failed_job_registry.remove(self, pipeline=pipeline)

        if delete_dependents:
            self.delete_dependents(pipeline=pipeline)

        connection.delete(self.key, self.dependents_key, self.dependencies_key)

    def delete_dependents(self, pipeline=None):
        """Delete jobs depending on this job."""
        connection = pipeline if pipeline is not None else self.connection
        for dependent_id in self.dependent_ids:
            try:
                job = Job.fetch(dependent_id, connection=self.connection)
                job.delete(pipeline=pipeline, remove_from_queue=False)
            except NoSuchJobError:
                # It could be that the dependent job was never saved to redis
                pass
        connection.delete(self.dependents_key)

    # Job execution
    def perform(self, invoke_callbacks=True, process_dependencies=True):  # pylint: disable=arguments-differ
        """Invokes the job function with the job arguments."""
        self.connection.persist(self.key)
        _job_stack.push(self)
        try:
            self._result = self._execute()
        except BaseException as err:
            if self.on_failure and invoke_callbacks:
                try:
                    self.on_failure(self, err)
                except BaseException as callback_exc:
                    logger.warning("Failed to execute on_failure() callback: {}".format(callback_exc), exc_info=True)
                    # noinspection PyBroadException
                    try:
                        import sentry_sdk

                        sentry_sdk.capture_exception(callback_exc)
                    except BaseException:
                        pass
            if process_dependencies:
                self.process_dependencies(JobStatus.FAILED)
            raise
        else:
            if self.on_success and invoke_callbacks:
                try:
                    self.on_success(self, self._result)
                except BaseException as callback_exc:
                    logger.warning("Failed to execute on_success() callback: {}".format(callback_exc), exc_info=True)
                    # noinspection PyBroadException
                    try:
                        import sentry_sdk

                        sentry_sdk.capture_exception(callback_exc)
                    except BaseException:
                        pass
            if process_dependencies:
                self.process_dependencies(JobStatus.FINISHED)
        finally:
            assert self is _job_stack.pop()
        return self._result

    def _execute(self):
        return self.func(*self.args, **self.kwargs)

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
        if self.func_name is None:
            return None

        arg_list = [as_text(truncate_long_string(repr(arg))) for arg in self.args]

        kwargs = ["{0}={1}".format(k, as_text(truncate_long_string(repr(v)))) for k, v in self.kwargs.items()]
        # Sort here because python 3.3 & 3.4 makes different call_string
        arg_list += sorted(kwargs)
        args = ", ".join(arg_list)

        return "{0}({1})".format(self.func_name, args)

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
    def failed_job_registry(self):
        from .registry import FailedJobRegistry

        return FailedJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)

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

    def register_dependency(self, pipeline=None):
        """Jobs may have dependencies. Jobs are enqueued only if the job they
        depend on is successfully performed. We record this relation as
        a reverse dependency (a Redis set), with a key that looks something
        like:

            rq:job:job_id:dependents = {'job_id_1', 'job_id_2'}

        This method adds the job in its dependency's dependents set
        and adds the job to DeferredJobRegistry.
        """
        from .registry import DeferredJobRegistry

        registry = DeferredJobRegistry(self.origin, connection=self.connection, job_class=self.__class__)
        registry.add(self, pipeline=pipeline)

        connection = pipeline if pipeline is not None else self.connection

        for dependency_id in self._dependency_ids:
            dependents_key = self.dependents_key_for(dependency_id)
            connection.sadd(dependents_key, self.id)
            connection.sadd(self.dependencies_key, dependency_id)

    @property
    def dependency_ids(self):
        dependencies = self.connection.smembers(self.dependencies_key)
        return [Job.key_for(_id.decode()) for _id in dependencies]

    def dependencies_are_met(self, exclude_job_id=None, pipeline=None):
        """Returns a boolean indicating if all of this jobs dependencies are _FINISHED_

        If a pipeline is passed, all dependencies are WATCHed.

        `exclude` allows us to exclude some job id from the status check. This is useful
        when enqueueing the dependents of a _successful_ job -- that status of
        `FINISHED` may not be yet set in redis, but said job is indeed _done_ and this
        method is _called_ in the _stack_ of it's dependents are being enqueued.
        """

        connection = pipeline if pipeline is not None else self.connection

        if pipeline is not None:
            connection.watch(*self.dependency_ids)

        dependencies_ids = {_id.decode() for _id in connection.smembers(self.dependencies_key)}

        if exclude_job_id:
            dependencies_ids.discard(exclude_job_id)

        with connection.pipeline() as pipeline:
            for key in dependencies_ids:
                pipeline.hget(self.key_for(key), "status")

            dependencies_statuses = pipeline.execute()

        return all(JobStatus.terminal(status.decode()) for status in dependencies_statuses if status)


_job_stack = LocalStack()


class Retry(object):
    def __init__(self, max, interval=0):
        """`interval` can be a positive number or a list of ints"""
        super().__init__()
        if max < 1:
            raise ValueError("max: please enter a value greater than 0")

        if isinstance(interval, int):
            if interval < 0:
                raise ValueError("interval: negative numbers are not allowed")
            intervals = [interval]
        elif isinstance(interval, Iterable):
            for i in interval:
                if i < 0:
                    raise ValueError("interval: negative numbers are not allowed")
            intervals = interval
        else:
            raise ValueError("Unknown type for intervals: {}".format(type(interval)))

        self.max = max
        self.intervals = intervals
