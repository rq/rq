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
import contextlib
from multiprocessing import Process

from redis import Redis
from rq import Connection, get_current_job, get_current_connection, Queue
from rq.decorators import job
from rq.compat import text_type
from rq.worker import HerokuWorker, Worker


def say_pid():
    return os.getpid()


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


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

def rpush(key, value, append_worker_name=False):
    """Push a value into a list in Redis. Useful for detecting the order in
    which jobs were executed."""
    if append_worker_name: value += ':' + get_current_job().worker_name
    redis = get_current_connection()
    redis.rpush(key, value)

def check_dependencies_are_met():
    return get_current_job().dependencies_are_met()

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


def start_worker(queue_name, conn_kwargs, worker_name, burst):
    """
    Start a worker. We accept only serializable args, so that this can be
    executed via multiprocessing.
    """
    # Silence stdout (thanks to <https://stackoverflow.com/a/28321717/14153673>)
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull):
            w = Worker([queue_name], name=worker_name, connection=Redis(**conn_kwargs))
            w.work(burst=burst)

def start_worker_process(queue_name, connection=None, worker_name=None, burst=False):
    """
    Use multiprocessing to start a new worker in a separate process.
    """
    connection = connection or get_current_connection()
    conn_kwargs = connection.connection_pool.connection_kwargs
    p = Process(target=start_worker, args=(queue_name, conn_kwargs, worker_name, burst))
    p.start()
    return p

def burst_two_workers(queue, timeout=2, tries=5, pause=0.1):
    """
    Get two workers working simultaneously in burst mode, on a given queue.
    """
    w1 = start_worker_process(queue.name, worker_name='w1', burst=True)
    w2 = Worker(queue, name='w2')
    jobs = queue.jobs
    if jobs:
        first_job = jobs[0]
        # Give the first worker process time to get started on the first job.
        # This is helpful in tests where we want to control which worker takes which job.
        n = 0
        while n < tries and not first_job.is_started:
            time.sleep(pause)
            n += 1
    # Now can start the second worker.
    w2.work(burst=True)
    w1.join(timeout)
