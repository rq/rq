# -*- coding: utf-8 -*-
"""
This file contains all jobs that are used in tests.  Each of these test
fixtures has a slighty different characteristics.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time

from rq import Connection, get_current_job
from rq.decorators import job
from rq.compat import PY2


def say_pid():
    return os.getpid()


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
    assert get_current_job() is not None


def echo(*args, **kwargs):
    return (args, kwargs)


class Number(object):
    def __init__(self, value):
        self.value = value

    @classmethod
    def divide(cls, x, y):
        return x * y

    def div(self, y):
        return self.value / y


class CallableObject(object):
    def __call__(self):
        return u"I'm callable"


class UnicodeStringObject(object):
    def __repr__(self):
        if PY2:
            return u'é'.encode('utf-8')
        else:
            return u'é'


with Connection():
    @job(queue='default')
    def decorated_job(x, y):
        return x + y


def black_hole(job, *exc_info):
    # Don't fall through to default behaviour (moving to failed queue)
    return False


def long_running_job(timeout=10):
    time.sleep(timeout)
    return 'Done sleeping...'
