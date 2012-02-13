import times
from uuid import uuid4
from cPickle import loads, dumps, UnpicklingError
from .proxy import conn
from .exceptions import UnpickleError, NoSuchJobError


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


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """

    # Job construction
    @classmethod
    def for_call(cls, func, *args, **kwargs):
        """Creates a new Job instance for the given function, arguments, and
        keyword arguments.
        """
        job = Job()
        job._func = func
        job._args = args
        job._kwargs = kwargs
        job.description = job.get_call_string()
        return job

    @property
    def func(self):
        return self._func

    @property
    def args(self):
        return self._args

    @property
    def kwargs(self):
        return self._kwargs

    @classmethod
    def fetch(cls, id):
        """Fetches a persisted job from its corresponding Redis key and
        instantiates it.
        """
        job = Job(id)
        job.refresh()
        return job

    def __init__(self, id=None):
        self._id = id
        self.created_at = times.now()
        self._func = None
        self._args = None
        self._kwargs = None
        self.description = None
        self.origin = None
        self.enqueued_at = None
        self.ended_at = None
        self.result = None
        self.exc_info = None


    # Data access
    def get_id(self):
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

    @property
    def key(self):
        """The Redis key that is used to store job data under."""
        return 'rq:job:%s' % (self.id,)


    @property
    def job_tuple(self):
        """Returns the job tuple that encodes the actual function call that this job represents."""
        return (self.func, self.args, self.kwargs)

    @property
    def return_value(self):
        """Returns the return value of the job.

        Initially, right after enqueueing a job, the return value will be None.
        But when the job has been executed, and had a return value or exception,
        this will return that value or exception.

        Note that, when the job has no return value (i.e. returns None), the
        ReadOnlyJob object is useless, as the result won't be written back to
        Redis.

        Also note that you cannot draw the conclusion that a job has _not_ been
        executed when its return value is None, since return values written back
        to Redis will expire after a given amount of time (500 seconds by
        default).
        """
        if self._cached_result is None:
            rv = conn.hget(self.key, 'result')
            if rv is not None:
                # cache the result
                self._cached_result = loads(rv)
        return self._cached_result


    # Persistence
    def refresh(self):
        """Overwrite the current instance's properties with the values in the
        corresponding Redis key.

        Will raise a NoSuchJobError if no corresponding Redis key exists.
        """
        key = self.key
        properties = ['data', 'created_at', 'origin', 'description',
                'enqueued_at', 'ended_at', 'result', 'exc_info']
        data, created_at, origin, description, \
                enqueued_at, ended_at, result, \
                exc_info = conn.hmget(key, properties)
        if data is None:
            raise NoSuchJobError('No such job: %s' % (key,))

        def to_date(date_str):
            if date_str is None:
                return None
            else:
                return times.to_universal(date_str)

        self._func, self._args, self._kwargs = unpickle(data)
        self.created_at = to_date(created_at)
        self.origin = origin
        self.description = description
        self.enqueued_at = to_date(enqueued_at)
        self.ended_at = to_date(ended_at)
        self.result = result
        self.exc_info = exc_info

    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        key = self.key

        obj = {}
        obj['created_at'] = times.format(self.created_at, 'UTC')

        if self.func is not None:
            obj['data'] = dumps(self.job_tuple)
        if self.origin is not None:
            obj['origin'] = self.origin
        if self.description is not None:
            obj['description'] = self.description
        if self.enqueued_at is not None:
            obj['enqueued_at'] = times.format(self.enqueued_at, 'UTC')
        if self.ended_at is not None:
            obj['ended_at'] = times.format(self.ended_at, 'UTC')
        if self.result is not None:
            obj['result'] = self.result
        if self.exc_info is not None:
            obj['exc_info'] = self.exc_info

        conn.hmset(key, obj)


    # Job execution
    def perform(self):
        """Invokes the job function with the job arguments.
        """
        return self.func(*self.args, **self.kwargs)


    # Representation
    def get_call_string(self):
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement.
        """
        if self.func is None:
            return None

        arg_list = [repr(arg) for arg in self.args]
        arg_list += ['%s=%r' % (k, v) for k, v in self.kwargs.items()]
        args = ', '.join(arg_list)
        return '%s(%s)' % (self.func.__name__, args)

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.description)


    # Job equality
    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

