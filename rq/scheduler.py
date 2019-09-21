import logging
import os
import signal
import time

from multiprocessing import Process

try:
    from signal import SIGKILL
except ImportError:
    from signal import SIGTERM as SIGKILL


from .job import Job
from .queue import Queue
from .registry import ScheduledJobRegistry
from .utils import current_timestamp, enum


SCHEDULER_KEY_TEMPLATE = 'rq:scheduler:%s'
SCHEDULER_LOCKING_KEY_TEMPLATE = 'rq:scheduler-lock:%s'

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


class RQScheduler(object):

    # STARTED: scheduler has been started but sleeping
    # WORKING: scheduler is in the midst of scheduling jobs
    # STOPPED: scheduler is in stopped condition

    Status = enum(
        'SchedulerStatus',
        STARTED='started',
        WORKING='working',
        STOPPED='stopped'
    )

    def __init__(self, queues, connection, interval=1):
        self._queue_names = set(parse_names(queues))
        self._acquired_locks = set([])
        self._scheduled_job_registries = []        
        self.connection = connection
        self.interval = interval
        self._stop_requested = False
        self._status = self.Status.STOPPED
        self._process = None
    
    @property
    def acquired_locks(self):
        return self._acquired_locks
    
    @property
    def status(self):
        return self._status

    def acquire_locks(self, auto_start=False):
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set([])
        pid = os.getpid()
        for name in self._queue_names:
            if self.connection.set(self.get_locking_key(name), pid, nx=True, ex=5):
                successful_locks.add(name)
        self._acquired_locks = self._acquired_locks.union(successful_locks)
        if self._acquired_locks:
            self.prepare_registries(self._acquired_locks)
        
        # If auto_start is requested and scheduler is not started,
        # run self.start()
        if self._acquired_locks and auto_start: 
            if not self._process:
                self.start()
        
        return successful_locks

    def prepare_registries(self, queue_names):
        """Prepare scheduled job registries for use"""
        self._scheduled_job_registries = []
        for name in queue_names:
            self._scheduled_job_registries.append(
                ScheduledJobRegistry(name, connection=self.connection)
            )

    @classmethod
    def get_locking_key(self, name):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_LOCKING_KEY_TEMPLATE % name

    def get_key(self, name):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_KEY_TEMPLATE % name

    def enqueue_scheduled_jobs(self):
        """Enqueue jobs whose timestamp is in the past"""
        self._status = self.Status.WORKING
        for registry in self._scheduled_job_registries:
            timestamp = current_timestamp()

            # TODO: try to use Lua script to make get_jobs_to_schedule()
            # and remove_jobs() atomic
            job_ids = registry.get_jobs_to_schedule(timestamp)

            if not job_ids:
                continue

            queue = Queue(registry.name, connection=self.connection)

            with self.connection.pipeline() as pipeline:
                # This should be done in bulk
                for job_id in job_ids:
                    job = Job.fetch(job_id, connection=self.connection)
                    queue.enqueue_job(job, pipeline=pipeline)
                registry.remove_jobs(timestamp)
                pipeline.execute()
        self._status = self.Status.STARTED

    def _install_signal_handlers(self):
        """Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """
        signal.signal(signal.SIGINT, self.request_stop)
        signal.signal(signal.SIGTERM, self.request_stop)

    def request_stop(self, signum=None, frame=None):
        """Toggle self._stop_requested that's checked on every loop"""
        self._stop_requested = True

    def heartbeat(self):
        """Updates the TTL on scheduler keys and the locks"""
        logging.info("Scheduler sending heartbeat to %s", ", ".join(self._queue_names))
        if len(self._queue_names) > 1:
            with self.connection.pipeline() as pipeline:
                for name in self._queue_names:
                    key = self.get_locking_key(name)
                    pipeline.expire(key, self.interval + 5)
                pipeline.execute()
        else:
            key = self.get_locking_key(next(iter(self._queue_names)))
            self.connection.expire(key, self.interval + 5)

    def stop(self):
        logging.info("Scheduler stopping, releasing locks for %s...",
                     ','.join(self._queue_names))
        keys = [self.get_locking_key(name) for name in self._queue_names]
        self.connection.delete(*keys)
        self._status = self.Status.STOPPED
    
    def start(self):
        self._status = self.Status.STARTED
        self._process = Process(target=run, args=(self,), name='Scheduler')
        self._process.start()
        return self._process

    def work(self):
        self._install_signal_handlers()
        while True:
            if self._stop_requested:
                self.stop()
                break
            self.enqueue_scheduled_jobs()
            self.heartbeat()
            time.sleep(self.interval)


def run(scheduler):
    logging.info("Scheduler for %s started with PID %s",
                 ','.join(scheduler._queue_names), os.getpid())
    scheduler.work()
    logging.info("Scheduler with PID %s has stopped", os.getpid())


def parse_names(queues_or_names):
    """Given a list of strings or queues, returns queue names"""
    names = []
    for queue_or_name in queues_or_names:
        if isinstance(queue_or_name, Queue):
            names.append(queue_or_name.name)
        else:
            names.append(str(queue_or_name))
    return names
