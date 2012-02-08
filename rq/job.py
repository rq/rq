import times
from uuid import uuid4
from pickle import loads, dumps
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
    except StandardError:
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
        job.func = func
        job.args = args
        job.kwargs = kwargs
        return job

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
        self.func = None
        self.args = None
        self.kwargs = None
        self.origin = None
        self.created_at = times.now()
        self.enqueued_at = None
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
        pickled_data = conn.hget(key, 'data')
        if pickled_data is None:
            raise NoSuchJobError('No such job: %s' % (key,))

        self.origin = conn.hget(key, 'origin')
        self.func, self.args, self.kwargs = unpickle(pickled_data)
        self.created_at = times.to_universal(conn.hget(key, 'created_at'))

    def save(self):
        """Persists the current job instance to its corresponding Redis key."""
        pickled_data = dumps(self.job_tuple)

        key = self.key
        conn.hset(key, 'data', pickled_data)
        conn.hset(key, 'origin', self.origin)
        conn.hset(key, 'created_at', times.format(self.created_at, 'UTC'))


    # Job execution
    def perform(self):
        """Invokes the job function with the job arguments.
        """
        return self.func(*self.args, **self.kwargs)


    # Representation
    @property
    def call_string(self):
        """Returns a string representation of the call, formatted as a regular
        Python function invocation statement.
        """
        arg_list = map(repr, self.args)
        arg_list += map(lambda tup: '%s=%r' % (tup[0], tup[1]),
                self.kwargs.items())
        return '%s(%s)' % (self.func.__name__, ', '.join(arg_list))

    def __str__(self):
        return '<Job %s: %s>' % (self.id, self.call_string)


    # Job equality
    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


    # TODO: TO REFACTOR / REMOVE
    def pickle(self):
        """Returns the pickle'd string represenation of a Job.  Suitable for
        writing to Redis.
        """
        return dumps(self)

