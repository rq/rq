from pickle import loads
from .exceptions import UnpickleError


class Job(object):
    """A Job is just a convenient datastructure to pass around job (meta) data.
    """
    @classmethod
    def unpickle(cls, pickle_data):
        """Constructs a Job instance form the given pickle'd job tuple data."""
        try:
            return loads(pickle_data)
        except (AttributeError, IndexError, TypeError):
            raise UnpickleError('Could not decode job tuple.')

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.rv_key = None
        self.origin = None
        self.timestamp = None

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
        return '<Job %s>' % self.call_string

