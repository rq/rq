import importlib
import inspect
import times
from uuid import uuid4
from cPickle import loads, dumps, UnpicklingError
from .connections import get_current_connection
from .exceptions import UnpickleError, NoSuchJobError


JOB_ATTRS = set(['origin', '_func_name', 'ended_at', 'description', '_args',
                 'created_at', 'enqueued_at', 'connection', '_result',
                 'timeout', '_kwargs', 'exc_info', '_id', 'data', '_instance'])


def unpickle(pickled_string):
    """Unpickles a string, but raises a unified UnpickleError in case anything
    fails.

    This is a helper method to not have to deal with the fact that `loads()`
    potentially raises many types of exceptions (e.g. AttributeError,
    IndexError, TypeError, KeyError, etc.)
    """
    try:
        obj = loads(pickled_string)
    except (StandardError, UnpicklingError):
        raise UnpickleError('Could not unpickle.', pickled_string)
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


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """

    # Job construction
    @classmethod
    def create(cls, func, args=None, kwargs=None, connection=None):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        assert isinstance(args, tuple), '%r is not a valid args list.' % (args,)
        assert isinstance(kwargs, dict), '%r is not a valid kwargs dict.' % (kwargs,)
        job = cls(connection=connection)
        if inspect.ismethod(func):
            job._instance = func.im_self
            job._func_name = func.__name__
        elif inspect.isfunction(func):
            job._func_name = '%s.%s' % (func.__module__, func.__name__)
        else:  # we expect a string
            job._func_name = func
        job._args = args
        job._kwargs = kwargs
        job.description = job.get_call_string()
        return job

    @property
    def func_name(self):
        return self._func_name

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
    def exists(cls, job_id):
        """Returns whether a job hash exists for the given job ID."""
        conn = get_current_connection()
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
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        self._id = id
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


    # Data access
    def get_id(self):  # noqa
        """The job ID for this job instance. Generates an ID lazily the
        first time the ID is requested.
        """
        if self._id is None:
            self._id = unicode(uuid4())
        return self._id

    def set_id(self, value):
        """Sets a job ID for the given job."""
        self._id = value

    id = property(get_id, set_id)

    @classmethod
    def key_for(cls, job_id):
        """The Redis key that is used to store job hash under."""
        return 'rq:job:%s' % (job_id,)

    @property
    def key(self):
        """The Redis key that is used to store job hash under."""
        return self.key_for(self.id)

    @property  # noqa
    def job_tuple(self):
        """Returns the job tuple that encodes the actual function call that
        this job represents."""
        return (self.func_name, self.instance, self.args, self.kwargs)

    @property
    def return_value(self):
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


    # Persistence
    def refresh(self):  # noqa
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        obj = self.connection.hgetall(key)
        if not obj:
            raise NoSuchJobError('No such job: %s' % (key,))

        def to_date(date_str):
            if date_str is None:
                return None
            else:
                return times.to_universal(date_str)

        self._func_name, self._instance, self._args, self._kwargs = unpickle(obj.get('data'))  # noqa
        self.created_at = to_date(obj.get('created_at'))
        self.origin = obj.get('origin')
        self.description = obj.get('description')
        self.enqueued_at = to_date(obj.get('enqueued_at'))
        self.ended_at = to_date(obj.get('ended_at'))
        self._result = unpickle(obj.get('result')) if obj.get('result') else None  # noqa
        self.exc_info = obj.get('exc_info')
        self.timeout = int(obj.get('timeout')) if obj.get('timeout') else None

        # Overwrite job's additional attrs (those not in JOB_ATTRS), if any
        additional_attrs = set(obj.keys()).difference(JOB_ATTRS)
        for attr in additional_attrs:
            setattr(self, attr, obj[attr])

    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        key = self.key

        obj = {}
        obj['created_at'] = times.format(self.created_at, 'UTC')

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
        """
        Store additional attributes from job instance into Redis. This is done
        so that third party libraries using RQ can store additional data
        directly on ``Job`` instances. For example:

        job = Job.create(func)
        job.foo = 'bar'
        job.save() # Will persist the 'foo' attribute
        """
        additional_attrs = set(self.__dict__.keys()).difference(JOB_ATTRS)
        for attr in additional_attrs:
            obj[attr] = getattr(self, attr)
        self.connection.hmset(key, obj)

    def cancel(self):
        """Cancels the given job, which will prevent the job from ever being
        ran (or inspected).

        This method merely exists as a high-level API call to cancel jobs
        without worrying about the internals required to implement job
        cancellation.  Technically, this call is (currently) the same as just
        deleting the job hash.
        """
        self.delete()

    def delete(self):
        """Deletes the job hash from Redis."""
        self.connection.delete(self.key)


    # Job execution
    def perform(self):  # noqa
        """Invokes the job function with the job arguments.
        """
        return self.func(*self.args, **self.kwargs)


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

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)


    # Job equality
    def __eq__(self, other):  # noqa
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)
