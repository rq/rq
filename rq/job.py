from datetime import datetime
from uuid import uuid4
from pickle import loads, dumps
from .exceptions import UnpickleError


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """
    @classmethod
    def unpickle(cls, pickle_data):
        """Constructs a Job instance form the given pickle'd job tuple data."""
        try:
            unpickled_obj = loads(pickle_data)
            assert isinstance(unpickled_obj, Job)
            return unpickled_obj
        except (AssertionError, AttributeError, IndexError, TypeError):
            raise UnpickleError('Could not unpickle Job.', pickle_data)

    def __init__(self, func, *args, **kwargs):
        self._id = unicode(uuid4())
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.origin = None
        self.created_at = datetime.utcnow()
        self.enqueued_at = None

    def pickle(self):
        """Returns the pickle'd string represenation of a Job.  Suitable for writing to Redis."""
        return dumps(self)

    @property
    def rv_key(self):
        """Returns the Redis key under which the Job's result will be stored, if applicable."""
        return 'rq:result:%s' % (self._id,)

    @property
    def id(self):
        """Returns the Job's internal ID."""
        return self._id

    def perform(self):
        """Invokes the job function with the job arguments.
        """
        return self.func(*self.args, **self.kwargs)

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

    def __eq__(self, other):
        return cmp(self.id, other.id)
