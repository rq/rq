"""
This file contains all jobs that are used in tests.  Each of these test
fixtures has a slighty different characteristics.
"""
import time
from . import find_empty_redis_database
from rq import Connection
from rq.decorators import job
from rq import get_current_job


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def do_nothing():
    """The best job in the world."""
    pass


def div_by_zero(x):
    """Prepare for a division-by-zero exception."""
    return x / 0


def some_calculation(x, y, z=1):
    """Some arbitrary calculation with three numbers.  Choose z smartly if you
    want a division by zero exception.
    """
    return x * y / z


def create_file(path):
    """Creates a file at the given path.  Actually, leaves evidence that the
    job ran."""
    with open(path, 'w') as f:
        f.write('Just a sentinel.')


def create_file_after_timeout(path, timeout):
    time.sleep(timeout)
    create_file(path)


def access_self():
    job = get_current_job()
    return job.id


class Number(object):
    def __init__(self, value):
        self.value = value

    @classmethod
    def divide(cls, x, y):
        return x * y

    def div(self, y):
        return self.value / y


with Connection(find_empty_redis_database()):
    @job(queue='default')
    def decorated_job(x, y):
        return x + y


def long_running_job():
    time.sleep(10)
