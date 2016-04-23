# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import inspect
import warnings
from functools import partial
from uuid import uuid4

from rq.compat import as_text, decode_redis_hash, string_types, text_type

from .exceptions import NoSuchJobError, UnpickleError
from .local import LocalStack
from .utils import enum, import_attribute, utcformat, utcnow, utcparse, transaction
from .keys import (job_key_from_id, children_key_from_id, queue_key_from_name,
                   QUEUES_KEY)

try:
    import cPickle as pickle
except ImportError:  # noqa
    import pickle

# Serialize pickle dumps using the highest pickle protocol (binary, default
# uses ascii)
dumps = partial(pickle.dumps, protocol=pickle.HIGHEST_PROTOCOL)
loads = pickle.loads


JobStatus = enum(
    'JobStatus',
    QUEUED='queued',
    FINISHED='finished',
    FAILED='failed',
    STARTED='started',
    DEFERRED='deferred'
)

# Sentinel value to mark that some of our lazily evaluated properties have not
# yet been evaluated.
UNEVALUATED = object()


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(pickled_string)
    except Exception as e:
        raise UnpickleError('Could not unpickle', pickled_string, e)
    return obj


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """

    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None,
               result_ttl=None, ttl=None, status=None, description=None,
               depends_on=None, timeout=None, origin='default', meta=None):
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

        job = cls(connection=connection)
        assert origin is not None, 'Must provide an origin'

        job.origin = origin

        # Set the core job tuple properties
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '{0}.{1}'.format(func.__module__, func.__name__)
        elif isinstance(func, string_types):
            job._func_name = as_text(func)
        elif not inspect.isclass(func) and hasattr(func, '__call__'):  # a callable class instance
            job._instance = func
            job._func_name = '__call__'
        else:
            raise TypeError('Expected a callable or a string, but got: {}'.format(func))
        job._args = args
        job._kwargs = kwargs

        # Extra meta data
        job.description = description or job.get_call_string()
        job.result_ttl = result_ttl
        job.ttl = ttl
        job.timeout = timeout
        job._status = status
        job.meta = meta or {}

        # dependencies could be a single job or a list of jobs
        if depends_on:
            if not isinstance(depends_on, (list, tuple)):
                depends_on = [depends_on]

            job._parent_ids = []
            for tmp in depends_on:
                if isinstance(tmp, Job):
                    job._parent_ids.append(tmp.id)
                else:
                    job._parent_ids.append(tmp)
        else:
            job._parent_ids = []

        return job

    @transaction
    def get_status(self):
        self._status = as_text(self.connection._hget(self.key, 'status'))
        return self._status

    def _get_status(self):
        warnings.warn(
            "job.status is deprecated. Use job.get_status() instead",
            DeprecationWarning
        )
        return self.get_status()

    @transaction
    def set_status(self, status):
        self._status = status
        self.connection._hset(self.key, 'status', self._status)

    def _set_status(self, status):

        warnings.warn(
            "job.status is deprecated. Use job.set_status() instead",
            DeprecationWarning
        )
        self.set_status(status)

    status = property(_get_status, _set_status)

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
    def parent_ids(self):
        """
        Returns a list of job's dependencies
        """
        return self._parent_ids

    @property
    @transaction
    def children(self):
        """
        Returns a list of jobs whose execution depends on this
        job's successful execution
        """
        children_ids = self.connection._smembers(self.children_key)
        return [self.connection.get_job(id) for id in children_ids]

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name)

        return import_attribute(self.func_name)

    def _unpickle_data(self):
        self._func_name, self._instance, self._args, self._kwargs = unpickle(self.data)

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
            self._data = dumps(job_tuple)
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
            self._unpickle_data()
        return self._func_name

    @func_name.setter
    def func_name(self, value):
        self._func_name = value
        self._data = UNEVALUATED

    @property
    def instance(self):
        if self._instance is UNEVALUATED:
            self._unpickle_data()
        return self._instance

    @instance.setter
    def instance(self, value):
        self._instance = value
        self._data = UNEVALUATED

    @property
    def args(self):
        if self._args is UNEVALUATED:
            self._unpickle_data()
        return self._args

    @args.setter
    def args(self, value):
        self._args = value
        self._data = UNEVALUATED

    @property
    def kwargs(self):
        if self._kwargs is UNEVALUATED:
            self._unpickle_data()
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = value
        self._data = UNEVALUATED

    def __init__(self, id=None, connection=None):
        from .connections import resolve_connection
        self.connection = resolve_connection(connection)

        if id is not None:
            self._id = id
        else:
            self._id = text_type(uuid4())

        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.started_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self.ttl = None
        self._status = None
        self._parent_ids = []
        self.meta = {}

    def __repr__(self):  # noqa
        return 'Job({0!r}, enqueued_at={1!r})'.format(self._id, self.enqueued_at)

    # Data access
    @property
    def id(self):
        """
        The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.
        """
        return self._id

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return job_key_from_id(self.id)

    @property
    def children_key(self):
        """The Redis key that is used to store job dependents hash under."""
        return children_key_from_id(self.id)

    @property
    @transaction
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
            rv = self.connection._hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._result = loads(rv)
        return self._result

    """Backwards-compatibility accessor property `return_value`."""
    return_value = result

    # Persistence
    @transaction
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        obj = decode_redis_hash(self.connection._hgetall(key))
        if len(obj) == 0:
            raise NoSuchJobError('No such job: {0}'.format(key))

        def to_date(date_str):
            if date_str is None:
                return
            else:
                return utcparse(as_text(date_str))

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        self.created_at = to_date(as_text(obj.get('created_at')))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = to_date(as_text(obj.get('enqueued_at')))
        self.started_at = to_date(as_text(obj.get('started_at')))
        self.ended_at = to_date(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = as_text(obj.get('exc_info'))
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None  # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._parent_ids = as_text(obj.get('parent_ids', '')).split(' ')
        self.ttl = int(obj.get('ttl')) if obj.get('ttl') else None
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

    def to_dict(self):
        """Returns a serialization of the current job instance"""
        obj = {}
        obj['created_at'] = utcformat(self.created_at or utcnow())
        obj['data'] = self.data

        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = utcformat(self.enqueued_at)
        if self.started_at is not None:
            obj['started_at'] = utcformat(self.started_at)
        if self.ended_at is not None:
            obj['ended_at'] = utcformat(self.ended_at)
        if self._result is not None:
            obj['result'] = dumps(self._result)
        if self.exc_info is not None:
            obj['exc_info'] = self.exc_info
        if self.timeout is not None:
            obj['timeout'] = self.timeout
        if self.result_ttl is not None:
            obj['result_ttl'] = self.result_ttl
        if self._status is not None:
            obj['status'] = self._status
        if self._parent_ids:
            obj['parent_ids'] = ' '.join(self._parent_ids)
        if self.meta:
            obj['meta'] = dumps(self.meta)
        if self.ttl:
            obj['ttl'] = self.ttl

        return obj

    @transaction
    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        # store as dict to determine whether the current job is even valid
        data = self.to_dict()
        key = self.key

        self.connection._hmset(key, data)
        self.cleanup(self.ttl)
        return self

    @transaction
    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """
        if self.origin:
            queue = self.connection.mkqueue(name=self.origin)
            queue.remove(self)

    @transaction
    def delete(self):
        """Cancels the job and deletes the job hash from Redis."""
        self.cancel()
        self._delete()

    def _delete(self):
        """Deletes the keys directly related to this job (does not touch queues
        or registries"""
        self.connection._delete(self.key)
        self.connection._delete(self.children_key)

    # Job execution
    def perform(self, default_result_ttl=90):
        """
        Invokes the job function with the job arguments.
        """
        self.connection._persist(self.key)

        self.ttl = -1
        _job_stack.push(self.id)
        exc = None
        try:
            self._result = self.func(*self.args, **self.kwargs)
        except Exception as exc:
            raise
        finally:
            self._job_ended(default_result_ttl, exc)
            assert self.id == _job_stack.pop()
        return self._result

    @transaction
    def _job_ended(self, default_result_ttl, exc_info):
        """Move job to finshed registry and set it to FINISHED"""
        finished_job_registry = self.connection.get_finished_registry(self.origin)
        started_job_registry = self.connection.get_started_registry(self.origin)
        failed_queue = self.connection.get_failed_queue()
        result_ttl = self.get_result_ttl(default_result_ttl)
        if exc_info is None:
            self._try_queue_children()
            self.set_status(JobStatus.FINISHED)

            # Remove our children list, any new dependencies should start
            # running immediately (based on JobStatus.FINISHED)
            self.connection._delete(self.children_key)

            # move from started to finished registry
            finished_job_registry.add(self, result_ttl)
        else:
            self.set_status(JobStatus.FAILED)
            failed_queue.quarantine(self, exc_info=exc_info)

        # Save results
        started_job_registry.remove(self)
        if result_ttl != 0:
            self.ended_at = utcnow()
            self.save()
            self.cleanup(result_ttl)
        else:
            self._delete()

    @transaction
    def _try_queue_children(self):
        # Now that the job is finished, check its dependents (children) to see
        # which are ready to start
        ready_jobs = []
        ready_queues = []
        ready_def_regs = []
        non_ready_jobs = []

        for child_id in self.connection._smembers(self.children_key):
            child = self.connection.get_job(child_id)
            if child._unfinished_parents(ignore={self.id}):
                non_ready_jobs.append(child)
            else:
                # Save jobs, queues and registries of jobs that we are about to
                # enqueue, since after we start writing we can't stop
                ready_jobs.append(child)
                ready_queues.append(self.connection.mkqueue(child.origin))
                ready_def_regs.append(self.connection.get_deferred_registry(child.origin))

        # These jobs are ready so they don't have any parents anymore -- that
        # key can be removed. The jobs queue should have the job_id added and
        # deferred
        for ready_job, ready_queue, ready_def_reg in zip(ready_jobs, ready_queues,
                                                         ready_def_regs):

            ready_def_reg.remove(ready_job)

            # enqueue the job
            ready_job.set_status(JobStatus.QUEUED)
            ready_job.enqueued_at = utcnow()
            if ready_job.timeout is None:
                ready_job.timeout = ready_queue.DEFAULT_TIMEOUT

            ready_job.save()

            # Todo store at_front in the job so that it skips the line here
            ready_queue.push_job_id(ready_job.id, at_front=False)

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

        arg_list = [as_text(repr(arg)) for arg in self.args]

        kwargs = ['{0}={1}'.format(k, as_text(repr(v))) for k, v in self.kwargs.items()]
        # Sort here because python 3.3 & 3.4 makes different call_string
        arg_list += sorted(kwargs)
        args = ', '.join(arg_list)

        return '{0}({1})'.format(self.func_name, args)

    @transaction
    def cleanup(self, ttl=None):
        """Prepare job for eventual deletion (if needed). This method is usually
        called after successful execution. How long we persist the job and its
        result depends on the value of ttl:
        - If ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If ttl is negative, don't set an expiry to it (persist
          forever)
        """
        if ttl == 0:
            self.delete()
        elif not ttl:
            return
        elif ttl > 0:
            self.connection._expire(self.key, ttl)

    @transaction
    def _unfinished_parents(self, ignore=[]):
        """
        Intended to be run on in-memory job (i.e. should not load or save from
        the job key itself)

        :return list: of job objects
        """
        unfinished_parents = []
        for parent_id in self._parent_ids:
            if parent_id in ignore:
                continue

            parent = self.connection.get_job(parent_id)
            if parent.get_status() != JobStatus.FINISHED:
                unfinished_parents.append(parent)

        return unfinished_parents

    @transaction
    def _add_child(self, child_id):
        """
        Adds a child job id to the list of jobs to update after this job
        finishes
        """
        self.connection._sadd(self.children_key, child_id)

    @transaction
    def _enqueue_or_deferr(self, at_front=False):
        """
        If job depends on an unfinished job, register itself on it's parent's
        dependents instead of enqueueing it.
        Otherwise enqueue the job
        """

        # check if all parents are done
        queue = self.connection.mkqueue(self.origin)
        parents_remaining = self._unfinished_parents()
        deferred = self.connection.get_deferred_registry(self.origin)

        # Make sure this job has actually been written
        if len(parents_remaining) > 0:
            # Update deferred registry, parent's children set and job
            self.set_status(JobStatus.DEFERRED)
            deferred.add(self)

            for parent in parents_remaining:
                parent._add_child(self.id)
        else:
            # Make sure the queue exists
            self.connection._sadd(QUEUES_KEY, queue.key)

            # enqueue the job
            self.set_status(JobStatus.QUEUED)
            self.enqueued_at = utcnow()
            if self.timeout is None:
                self.timeout = queue.DEFAULT_TIMEOUT

            self.save()
            queue.push_job_id(self.id, at_front=at_front)

    def __str__(self):
        return '<Job {0}: {1}>'.format(self.id, self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return isinstance(other, self.__class__) and self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @property
    def rq_conn(self):
        """
        Used by transaction to create pipes
        """
        return self.connection

_job_stack = LocalStack()
