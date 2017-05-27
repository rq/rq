import signal
import time
from datetime import datetime

try:
    from logbook import Logger
    Logger = Logger   # Does nothing except it shuts up pyflakes annoying error
except ImportError:
    from logging import Logger

from .connections import get_current_connection
from .job import Job
from .queue import Queue


class Scheduler(object):
    scheduler_key = 'rq:scheduler'
    scheduled_jobs_key = 'rq:scheduler:scheduled_jobs'

    def __init__(self, queue_name='default', interval=60, connection=None):
        if connection is None:
            connection = get_current_connection()
        self.connection = connection
        self.queue_name = queue_name
        self._interval = interval
        self.log = Logger('scheduler')

    def register_birth(self):
        if self.connection.exists(self.scheduler_key) and \
                not self.connection.hexists(self.scheduler_key, 'death'):
            raise ValueError("There's already an active RQ scheduler")
        key = self.scheduler_key
        now = time.time()
        with self.connection.pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        with self.connection.pipeline() as p:
            p.hset(self.scheduler_key, 'death', time.time())
            p.expire(self.scheduler_key, 60)
            p.execute()

    def _install_signal_handlers(self):
        """
        Installs signal handlers for handling SIGINT and SIGTERM
        gracefully.
        """

        def stop(signum, frame):
            """
            Register scheduler's death and exit.
            """
            self.log.debug('Shutting down RQ scheduler...')
            self.register_death()
            raise SystemExit()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)


    def schedule(self, scheduled_time, func, *args, **kwargs):
        """
        Pushes a job to the scheduler queue. The scheduled queue is a Redis sorted
        set ordered by timestamp - which in this case is job's scheduled execution time.

        Usage:
        
        from datetime import datetime
        from redis import Redis
        from rq.scheduler import Scheduler

        from foo import func

        redis = Redis()
        scheduler = Scheduler(queue_name='default', connection=redis)
        scheduler.schedule(datetime(2020, 1, 1), func, 'argument', keyword='argument')
        """
        if func.__module__ == '__main__':
            raise ValueError(
                    'Functions from the __main__ module cannot be processed '
                    'by workers.')

        job = Job.create(func, *args, connection=self.connection, **kwargs)
        job.origin = self.queue_name
        job.save()
        self.connection.zadd(self.scheduled_jobs_key, job.id,
                             int(scheduled_time.strftime('%s')))
        return job

    def get_jobs_to_queue(self):
        """
        Returns a list of job instances that should be queued
        (score lower than current timestamp).
        """
        job_ids = self.connection.zrangebyscore(self.scheduled_jobs_key, 0, int(time.strftime('%s')))
        return [Job.fetch(job_id, connection=self.connection) for job_id in job_ids]

    def get_queue_for_job(self, job):
        """
        Returns a queue to put job into.
        """
        key = '{0}{1}'.format(Queue.redis_queue_namespace_prefix, job.origin)
        return Queue.from_queue_key(key, connection=self.connection)

    def enqueue_job(self, job):
        """
        Move a scheduled job to a queue.
        """
        job.enqueued_at = datetime.now()
        job.save()
        queue = self.get_queue_for_job(job)
        queue.push_job_id(job.id)
        self.connection.zrem(self.scheduled_jobs_key, job.id)

    def enqueue_jobs(self):
        """
        Move scheduled jobs into queues. 
        """
        jobs = self.get_jobs_to_queue()
        for job in jobs:
            self.enqueue_job(job)
        return jobs

    def run(self):
        """
        Periodically check whether there's any job that should be put in the queue (score 
        lower than current time).
        """
        self.register_birth()
        self._install_signal_handlers()
        try:
            while True:
                self.enqueue_jobs()
                time.sleep(self._interval)
        finally:
            self.register_death()
