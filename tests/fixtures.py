"""
This file contains all jobs that are used in tests.  Each of these test
fixtures has a slighty different characteristics.
"""
import time

from rq.decorators import job


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


class Calculator(object):
    """Test instance methods."""
    def __init__(self, denominator):
        self.denominator = denominator

    def calculate(x, y):
        return x * y / self.denominator

@job()
def decorated_job(x, y):
    return x + y
