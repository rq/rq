import importlib
import inspect
import times
from uuid import uuid4
try:
    from cPickle import loads, dumps, UnpicklingError
except ImportError:  # noqa
    from pickle import loads, dumps, UnpicklingError  # noqa
from .local import LocalStack
from .connections import resolve_connection
from .exceptions import UnpickleError, NoSuchJobError
from .utils import parse_func
from rq.compat import text_type, decode_redis_hash, as_text


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)
    return type(name, (), values)

Status = enum('Status',
              QUEUED='queued', FINISHED='finished', FAILED='failed',
              STARTED='started')


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


def get_current_job():
    """Returns the Job instance that is currently being executed.  If this
    function is invoked from outside a job context, None is returned.
    """
    job_id = _job_stack.top
    if job_id is None:
        return None
    return Job.fetch(job_id)


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """
    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None,
               result_ttl=None, status=None, description=None, dependency=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        assert isinstance(args, (tuple, list)), '%r is not a valid args list.' % (args,)
        assert isinstance(kwargs, dict), '%r is not a valid kwargs dict.' % (kwargs,)
        job = cls(connection=connection)
        if inspect.ismethod(func):
            job._instance = func.__self__
            job._func_name = func.__name__
        elif inspect.isfunction(func) or inspect.isbuiltin(func):
            job._func_name = '%s.%s' % (func.__module__, func.__name__)
        else:  # we expect a string
            job._func_name = func
        job._args = args
        job._kwargs = kwargs
        job.description = description or job.get_call_string()
        job.result_ttl = result_ttl
        job._status = status
        # dependency could be job instance or id
        if dependency is not None:
            job._dependency_id = dependency.id if isinstance(dependency, Job) else dependency
        return job

    @property
    def func_name(self):
        return self._func_name

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
    def dependency(self):
        """Returns a job's dependency. To avoid repeated Redis fetches, we cache
        job.dependency as job._dependency.
        """
        if self._dependency_id is None:
            return None
        if hasattr(self, '_dependency'):
            return self._dependency
        job = Job.fetch(self._dependency_id, connection=self.connection)
        job.refresh()
        self._dependency = job
        return job

    @property
    def func(self):
        func_name = self.func_name
        if func_name is None:
            return None

        if self.instance:
            return getattr(self.instance, func_name)

        module_name, func_name = func_name.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    @property
    def instance(self):
        return self._instance

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

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

    @classmethod
    def safe_fetch(cls, id, connection=None):
        """Fetches a persisted job from its corresponding Redis key, but does
        not instantiate it, making it impossible to get UnpickleErrors.
        """
        job = cls(id, connection=connection)
        job.refresh(safe=True)
        return job

    def __init__(self, id=None, connection=None):
        self.connection = resolve_connection(connection)
        self._id = as_text(id)
        self.created_at = times.now()
        self._func_name = None
        self._instance = None
        self._args = None
        self._kwargs = None
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.ended_at = None
        self._result = None
        self.exc_info = None
        self.timeout = None
        self.result_ttl = None
        self._status = None
        self._dependency_id = None
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
        self._id = as_text(value)

    id = property(get_id, set_id)

    @classmethod
    def key_for(cls, job_id):
        """The Redis key that is used to store job hash under."""
        return b'rq:job:' + as_text(job_id).encode('utf-8')

    @classmethod
    def waitlist_key_for(cls, job_id):
        """The Redis key that is used to store job hash under."""
        return b'rq:job:' + as_text(job_id).encode('utf-8') + b':waitlist'

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return self.key_for(self.id)

    @property
    def waitlist_key(self):
        """The Redis key that is used to store job hash under."""
        return self.waitlist_key_for(self.id)

    @property  # noqa
    def job_tuple(self):
        """Returns the job tuple that encodes the actual function call that
        this job represents."""
        return (self.func_name, self.instance, self.args, self.kwargs)

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
    def refresh(self, safe=False):  # noqa
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
                return None
            else:
                return times.to_universal(as_text(date_str))

        try:
            self.data = obj['data']
        except KeyError:
            raise NoSuchJobError('Unexpected job format: {0}'.format(obj))

        try:
            self._func_name, self._instance, self._args, self._kwargs = unpickle(self.data)
        except UnpickleError:
            if not safe:
                raise
        self.created_at = to_date(as_text(obj.get('created_at')))
        self.origin = as_text(obj.get('origin'))
        self.description = as_text(obj.get('description'))
        self.enqueued_at = to_date(as_text(obj.get('enqueued_at')))
        self.ended_at = to_date(as_text(obj.get('ended_at')))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = obj.get('exc_info')
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None
        self.result_ttl = int(obj.get('result_ttl')) if obj.get('result_ttl') else None # noqa
        self._status = as_text(obj.get('status') if obj.get('status') else None)
        self._dependency_id = as_text(obj.get('dependency_id', None))
        self.meta = unpickle(obj.get('meta')) if obj.get('meta') else {}

    def dump(self):
        """Returns a serialization of the current job instance"""
        obj = {}
        obj['created_at'] = times.format(self.created_at or times.now(), 'UTC')

        if self.func_name is not None:
            obj['data'] = dumps(self.job_tuple)
        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = times.format(self.enqueued_at, 'UTC')
        if self.ended_at is not None:
            obj['ended_at'] = times.format(self.ended_at, 'UTC')
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
        if self._dependency_id is not None:
            obj['dependency_id'] = self._dependency_id
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
        ran (or inspected). This function will also delete references to this
        job from associated scheduler or queue.

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.
        """
        # If a job gets deleted, it should remove itself from queue and scheduler
        from .queue import Queue, Status
        from .scheduler import Scheduler

        pipeline = self.connection._pipeline()
        self.delete(pipeline=pipeline)

        scheduler = Scheduler(connection=self.connection)
        if self in scheduler:
            scheduler.cancel(self, pipeline=pipeline)

        if self._status == Status.QUEUED:
            queue = Queue(self.origin, self.connection)
            pipeline.lrem(queue.key, 1, self.id)

        pipeline.execute()

    def delete(self, pipeline=None):
        """Deletes the job hash from Redis."""
        connection = self.connection if pipeline is None else pipeline
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

    def register_dependency(self):
        """Jobs may have a waitlist. Jobs in this waitlist are enqueued
        only if the dependency job is successfully performed. We maintain this
        waitlist in Redis, with key that looks something like:
            
            rq:job:job_id:waitlist = ['job_id_1', 'job_id_2']
        
        This method puts the job on it's dependency's waitlist.
        """
        # TODO: This can probably be pipelined
        self.connection.rpush(Job.waitlist_key_for(self._dependency_id), self.id)

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)


    # Job equality
    def __eq__(self, other):  # noqa
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

_job_stack = LocalStack()
