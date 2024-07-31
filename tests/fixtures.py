"""
This file contains all jobs that are used in tests.  Each of these test
fixtures has a slightly different characteristics.
"""

import os
import signal
import subprocess
import sys
import time
from multiprocessing import Process
from typing import Optional

from redis import Redis

from rq import Queue, get_current_job
from rq.command import send_kill_horse_command, send_shutdown_command
from rq.defaults import DEFAULT_JOB_MONITORING_INTERVAL
from rq.job import Job
from rq.suspension import resume
from rq.worker import HerokuWorker, Worker


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
    return str(say_hello(name))  # noqa


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


def long_process():
    time.sleep(60)
    return


def some_calculation(x, y, z=1):
    """Some arbitrary calculation with three numbers.  Choose z smartly if you
    want a division by zero exception.
    """
    return x * y / z


def rpush(key, value, connection_kwargs: dict, append_worker_name=False, sleep=0):
    """Push a value into a list in Redis. Useful for detecting the order in
    which jobs were executed."""
    if sleep:
        time.sleep(sleep)
    if append_worker_name:
        value += ':' + get_current_job().worker_name
    redis = Redis(**connection_kwargs)
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


def create_file_after_timeout_and_setpgrp(path, timeout):
    os.setpgrp()
    create_file_after_timeout(path, timeout)


def launch_process_within_worker_and_store_pid(path, timeout):
    p = subprocess.Popen(['sleep', str(timeout)])
    with open(path, 'w') as f:
        f.write('{}'.format(p.pid))
    p.wait()


def access_self():
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


class Number:
    def __init__(self, value):
        self.value = value

    @classmethod
    def divide(cls, x, y):
        return x * y

    def div(self, y):
        return self.value / y


class CallableObject:
    def __call__(self):
        return u"I'm callable"


class UnicodeStringObject:
    def __repr__(self):
        return u'é'


class ClassWithAStaticMethod:
    @staticmethod
    def static_method():
        return u"I'm a static method"


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


def run_dummy_heroku_worker(sandbox, _imminent_shutdown_delay, connection):
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

    w = TestHerokuWorker(Queue('dummy', connection=connection), connection=connection)
    w.main_work_horse(None, None)


class DummyQueue:
    pass


def kill_worker(pid: int, double_kill: bool, interval: float = 1.5):
    # wait for the worker to be started over on the main process
    time.sleep(interval)
    os.kill(pid, signal.SIGTERM)
    if double_kill:
        # give the worker time to switch signal handler
        time.sleep(interval)
        os.kill(pid, signal.SIGTERM)


def resume_worker(connection_kwargs: dict, interval: float = 1):
    # Wait and resume RQ
    time.sleep(interval)
    resume(Redis(**connection_kwargs))


class Serializer:
    def loads(self):
        pass

    def dumps(self):
        pass


def start_worker(queue_name, conn_kwargs, worker_name, burst, job_monitoring_interval=None):
    """
    Start a worker. We accept only serializable args, so that this can be
    executed via multiprocessing.
    """
    # Silence stdout (thanks to <https://stackoverflow.com/a/28321717/14153673>)
    # with open(os.devnull, 'w') as devnull:
    #     with contextlib.redirect_stdout(devnull):
    w = Worker(
        [queue_name],
        name=worker_name,
        connection=Redis(**conn_kwargs),
        job_monitoring_interval=job_monitoring_interval or DEFAULT_JOB_MONITORING_INTERVAL,
    )
    w.work(burst=burst)


def start_worker_process(
    queue_name, connection, worker_name=None, burst=False, job_monitoring_interval: Optional[int] = None
) -> Process:
    """
    Use multiprocessing to start a new worker in a separate process.
    """
    connection = connection
    conn_kwargs = connection.connection_pool.connection_kwargs
    p = Process(target=start_worker, args=(queue_name, conn_kwargs, worker_name, burst, job_monitoring_interval))
    p.start()
    return p


def burst_two_workers(queue, connection: Redis, timeout=2, tries=5, pause=0.1):
    """
    Get two workers working simultaneously in burst mode, on a given queue.
    Return after both workers have finished handling jobs, up to a fixed timeout
    on the worker that runs in another process.
    """
    w1 = start_worker_process(queue.name, worker_name='w1', burst=True, connection=connection)
    w2 = Worker(queue, name='w2', connection=connection)
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


def save_result(job, connection, result):
    """Store job result in a key"""
    connection.set('success_callback:%s' % job.id, result, ex=60)


def save_exception(job, connection, type, value, traceback):
    """Store job exception in a key"""
    connection.set('failure_callback:%s' % job.id, str(value), ex=60)


def save_result_if_not_stopped(job, connection, result=""):
    connection.set('stopped_callback:%s' % job.id, result, ex=60)


def erroneous_callback(job):
    """A callback that's not written properly"""
    pass


def _send_shutdown_command(worker_name, connection_kwargs, delay=0.25):
    time.sleep(delay)
    send_shutdown_command(Redis(**connection_kwargs), worker_name)


def _send_kill_horse_command(worker_name, connection_kwargs, delay=0.25):
    """Waits delay before sending kill-horse command"""
    time.sleep(delay)
    send_kill_horse_command(Redis(**connection_kwargs), worker_name)


class CustomJob(Job):
    """A custom job class just to test it"""
