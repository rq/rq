import inspect
from uuid import uuid4
try:
    from cPickle import loads, dumps, UnpicklingError
except ImportError:  # noqa
    from pickle import loads, dumps, UnpicklingError  # noqa
from .local import LocalStack
from .connections import resolve_connection
from .exceptions import UnpickleError, NoSuchJobError
from .utils import import_attribute, utcnow, utcformat, utcparse
from rq.compat import text_type, decode_redis_hash, as_text

from redis import WatchError


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(name, (), values)

Status = enum('Status',
              QUEUED='queued', FINISHED='finished', FAILED='failed',
              STARTED='started')

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
    except (Exception, UnpicklingError) as e:
        raise UnpickleError('Could not unpickle.', pickled_string, e)
    return obj


def cancel_job(job_id, connection=None):
    """Cancels the job with the given job ID, preventing execution.  Discards
    any job info (i.e. it can't be requeued later).
    """
    Job(job_id, connection=connection).cancel()


def requeue_job(job_id, connection=None):
    """Requeues the job with the given job ID.  The job ID should refer to
    a failed job (i.e. it should be on the failed queue).  If no such (failed)
    job exists, a NoSuchJobError is raised.
    """
    from .queue import get_failed_queue
    fq = get_failed_queue(connection=connection)
    fq.requeue(job_id)


def get_current_job(connection=None):
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = _job_stack.top
    if job_id is None:
        return None
    return Job.fetch(job_id, connection=connection)


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """

    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None,
               result_ttl=None, status=None, description=None, depends_on=None, timeout=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        if not isinstance(args, (tuple, list)):
            raise TypeError('{0!r} is not a valid args list.'.format(args))
        if not isinstance(kwargs, dict):
            raise TypeError('{0!r} is not a valid kwargs dict.'.format(kwargs))

        job = cls(connection=connection)

        # Set the core job tuple properties
        job._instance = None
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '%s.%s' % (func.__module__, func.__name__)
        else:  # we expect a string
            job._func_name = func
        job._args = args
        job._kwargs = kwargs

        # Extra meta data
        job.description = description or job.get_call_string()
        job.result_ttl = result_ttl
        job.timeout = timeout
        job._status = status

        # depends_on could be a job instance or job id, or list thereof
        if depends_on is not None:
            if isinstance(depends_on, (Job, text_type)):
                depends_on = [depends_on]
            job._dependency_ids = list(
                dependency.id if isinstance(dependency, Job) else dependency for dependency in depends_on)
        return job

    def _get_status(self):
        self._status = as_text(self.connection.hget(self.key, 'status'))
        return self._status

    def _set_status(self, status):
        self._status = status
        self.connection.hset(self.key, 'status', self._status)

    status = property(_get_status, _set_status)

    @property
    def is_finished(self):
        return self.status == Status.FINISHED

    @property
    def is_queued(self):
        return self.status == Status.QUEUED

    @property
    def is_failed(self):
        return self.status == Status.FAILED

    @property
    def is_started(self):
        return self.status == Status.STARTED

    @property
    def dependencies(self):
        """Returns a job's dependencies. To avoid repeated Redis fetches, we
        cache job.dependencies as job._dependencies.
        TODO: What if the dependency has already been removed e.g. due to result_ttl timeout?
        """
        try:
            return self._dependencies
        except AttributeError:
            self._dependencies = [Job.fetch(dependency_id) for dependency_id in self._dependency_ids]
            return self._dependencies

    def remove_dependency(self, dependency_id):
        """Removes a dependency from job. This is usually called when
        dependency is successfully executed."""
        # TODO: can probably be pipelined
        self.connection.srem(self.remaining_dependencies_key, dependency_id)

    def has_unmet_dependencies(self):
        """Checks whether job has dependencies that aren't yet finished."""
        return bool(self.connection.scard(self.remaining_dependencies_key))

    @property
    def reverse_dependencies(self):
        """Returns a list of jobs whose execution depends on this
        job's successful execution"""
        reverse_dependency_ids = self.connection.smembers(self.reverse_dependencies_key)
        return [Job.fetch(id) for id in reverse_dependency_ids]

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
                raise ValueError('Cannot build the job data.')

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

    @classmethod
    def exists(cls, job_id, connection=None):
        """Returns whether a job hash exists for the given job ID."""
        conn = resolve_connection(connection)
        return conn.exists(cls.key_for(job_id))

    @classmethod
    def fetch(cls, id, connection=None):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = cls(id, connection=connection)
        job.refresh()
        return job

    def __init__(self, id=None, connection=None):
        self.connection = resolve_connection(connection)
        self._id = id
        self.created_at = utcnow()
        self._data = UNEVALUATED
        self._func_name = UNEVALUATED
        self._instance = UNEVALUATED
        self._args = UNEVALUATED
        self._kwargs = UNEVALUATED
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self._status = None
        self._dependency_ids = []
        self.meta = {}

    # Data access
    def get_id(self):  # noqa
        """The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.
        """
        if self._id is None:
            self._id = text_type(uuid4())
        return self._id

    def set_id(self, value):
        """Sets a job ID for the given job."""
        self._id = value

    id = property(get_id, set_id)

    @classmethod
    def key_for(cls, job_id):
        """Redis key for the job hash."""
        return b'rq:job:' + job_id.encode('utf-8')

    @classmethod
    def reverse_dependencies_key_for(cls, job_id):
        """Redis key for the dependent job set."""
        return b'rq:job:' + job_id.encode('utf-8') + b':reverse_dependencies'

    @classmethod
    def remaining_dependencies_key_for(cls, job_id):
        """Redis key for the dependency job set."""
        return b'rq:job:' + job_id.encode('utf-8') + b':dependencies'

    @property
    def key(self):
        """Redis key for the job hash."""
        return self.key_for(self.id)

    @property
    def reverse_dependencies_key(self):
        """Redis key for the dependent job set."""
        return self.reverse_dependencies_key_for(self.id)

    @property
    def remaining_dependencies_key(self):
        """Redis key for the dependency job set."""
        return self.remaining_dependencies_key_for(self.id)

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
                self._result = loads(rv)
        return self._result

    """Backwards-compatibility accessor property `return_value`."""
    return_value = result

    # Persistence
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        obj = decode_redis_hash(self.connection.hgetall(key))
        if len(obj) == 0:
            raise NoSuchJobError('No such job: %s' % (key,))

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
        self.ended_at = to_date(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = as_text(obj.get('exc_info'))
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None  # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._dependency_ids = as_text(obj.get('dependency_ids', '')).split(' ')
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

    def dump(self):
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
        if self._dependency_ids:
            obj['dependency_ids'] = ' '.join(self._dependency_ids)
        if self.meta:
            obj['meta'] = dumps(self.meta)

        return obj

    def save(self, pipeline=None):
        """Persists the current job instance to its corresponding Redis key."""
        key = self.key
        connection = pipeline if pipeline is not None else self.connection

        connection.hmset(key, self.dump())

    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.  Technically, this call is (currently) the same as just
        deleting the job hash.

        NOTE: Any job that depends on this job becomes orphaned.
        """
        pipeline = self.connection.pipeline()
        self.delete(pipeline=pipeline)
        pipeline.delete(self.reverse_dependencies_key)
        pipeline.execute()

    def delete(self, pipeline=None):
        """Deletes the job hash from Redis."""
        connection = pipeline if pipeline is not None else self.connection
        connection.delete(self.key)

    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments."""
        _job_stack.push(self.id)
        try:
            self._result = self.func(*self.args, **self.kwargs)
        finally:
            assert self.id == _job_stack.pop()
        return self._result

    def get_ttl(self, default_ttl=None):
        """Returns ttl for a job that determines how long a job and its result
        will be persisted. In the future, this method will also be responsible
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

        arg_list = [repr(arg) for arg in self.args]
        arg_list += ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)
        return '%s(%s)' % (self.func_name, args)

    def cleanup(self, ttl=None, pipeline=None):
        """Prepare job for eventual deletion (if needed). This method is usually
        called after successful execution. How long we persist the job and its
        result depends on the value of result_ttl:
        - If result_ttl is 0, cleanup the job immediately.
        - If it's a positive number, set the job to expire in X seconds.
        - If result_ttl is negative, don't set an expiry to it (persist
          forever)
        """
        if ttl == 0:
            self.cancel()
        elif ttl > 0:
            connection = pipeline if pipeline is not None else self.connection
            connection.expire(self.key, ttl)

    def register_dependencies(self, dependencies):
        """Register this job as being dependent on its dependencies.
        A job is added to its queue only if all its dependencies have succeeded.

        For each unmet dependency (jobs that aren't successfully completed), we
        register this job's id in a Redis set:

            rq:job:job_id:reverse_dependencies = {'job_id_1', 'job_id_2'}
        """
        remaining_dependencies = []
        with self.connection.pipeline() as pipeline:
            while True:
                try:
                    # Check whether any of dependencies have been met
                    # pipeline.watch() is used to ensure that no dependency 
                    # is modified in the duration of the check
                    # TODO: Each dependency.status call issues a Redis query
                    # We should probably use bulk fetches if possible
                    pipeline.watch(*[dependency.key for dependency in dependencies])
                    for dependency in dependencies:
                        if dependency.status != Status.FINISHED:
                            remaining_dependencies.append(dependency)

                    if remaining_dependencies:
                        pipeline.multi()
                        pipeline.sadd(self.remaining_dependencies_key,
                                      *[dependency.id for dependency in remaining_dependencies])
            
                        for dependency in remaining_dependencies:
                            pipeline.sadd(Job.reverse_dependencies_key_for(dependency.id), self.id)
                    
                        pipeline.execute()
                    break
                except WatchError:
                    continue

        return remaining_dependencies

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)

    # Job equality
    def __eq__(self, other):  # noqa
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

_job_stack = LocalStack()
