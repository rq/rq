# -*- coding: utf-8 -*-
"""
This file contains all jobs that are used in tests.  Each of these test
fixtures has a slighty different characteristics.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time
import signal
import sys
import subprocess

from rq import Connection, get_current_job, get_current_connection, Queue
from rq.decorators import job
from rq.compat import text_type
from rq.worker import HerokuWorker


def say_pid():
    return os.getpid()


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


async def say_hello_async(name=None):
    """A async job with a single argument and a return value."""
    return say_hello(name)


def say_hello_unicode(name=None):
    """A job with a single argument and a return value."""
    return text_type(say_hello(name))  # noqa


def do_nothing():
    """The best job in the world."""
    pass

def raise_exc():
    raise Exception('raise_exc error')

def raise_exc_mock():
    return raise_exc

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

def create_file_after_timeout_and_setsid(path, timeout):
    os.setsid()
    create_file_after_timeout(path, timeout)

def launch_process_within_worker_and_store_pid(path, timeout):

    p = subprocess.Popen(['sleep', str(timeout)])
    with open(path, 'w') as f:
        f.write('{}'.format(p.pid))

    p.wait()

def access_self():
    assert get_current_connection() is not None
    assert get_current_job() is not None


def modify_self(meta):
    j = get_current_job()
    j.meta.update(meta)
    j.save()


def modify_self_and_error(meta):
    j = get_current_job()
    j.meta.update(meta)
    j.save()
    return 1 / 0


def echo(*args, **kwargs):
    return args, kwargs


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
        return u'é'


with Connection():
    @job(queue='default')
    def decorated_job(x, y):
        return x + y


def black_hole(job, *exc_info):
    # Don't fall through to default behaviour (moving to failed queue)
    return False


def add_meta(job, *exc_info):
    job.meta = {'foo': 1}
    job.save()
    return True


def save_key_ttl(key):
    # Stores key ttl in meta
    job = get_current_job()
    ttl = job.connection.ttl(key)
    job.meta = {'ttl': ttl}
    job.save_meta()


def long_running_job(timeout=10):
    time.sleep(timeout)
    return 'Done sleeping...'


def run_dummy_heroku_worker(sandbox, _imminent_shutdown_delay):
    """
    Run the work horse for a simplified heroku worker where perform_job just
    creates two sentinel files 2 seconds apart.
    :param sandbox: directory to create files in
    :param _imminent_shutdown_delay: delay to use for HerokuWorker
    """
    sys.stderr = open(os.path.join(sandbox, 'stderr.log'), 'w')

    class TestHerokuWorker(HerokuWorker):
        imminent_shutdown_delay = _imminent_shutdown_delay

        def perform_job(self, job, queue):
            create_file(os.path.join(sandbox, 'started'))
            # have to loop here rather than one sleep to avoid holding the GIL
            # and preventing signals being received
            for i in range(20):
                time.sleep(0.1)
            create_file(os.path.join(sandbox, 'finished'))

    w = TestHerokuWorker(Queue('dummy'))
    w.main_work_horse(None, None)


class DummyQueue(object):
    pass


def kill_worker(pid, double_kill, interval=0.5):
    # wait for the worker to be started over on the main process
    time.sleep(interval)
    os.kill(pid, signal.SIGTERM)
    if double_kill:
        # give the worker time to switch signal handler
        time.sleep(interval)
        os.kill(pid, signal.SIGTERM)


class Serializer(object):
    def loads(self): pass

    def dumps(self): pass

