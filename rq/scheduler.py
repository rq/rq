import logging
import signal
import time
import times

from datetime import datetime, timedelta
from itertools import repeat

from .connections import resolve_connection
from .exceptions import NoSuchJobError
from .job import Job
from .queue import Queue

from redis import WatchError


logger = logging.getLogger(__name__)


class Scheduler(object):
    scheduler_key = 'rq:scheduler'
    scheduled_jobs_key = 'rq:scheduler:scheduled_jobs'

    def __init__(self, queue_name='default', interval=60, connection=None):
        self.connection = resolve_connection(connection)
        self.queue_name = queue_name
        self._interval = interval
        self.log = logger

    def register_birth(self):
        if self.connection.exists(self.scheduler_key) and \
                not self.connection.hexists(self.scheduler_key, 'death'):
            raise ValueError("There's already an active RQ scheduler")
        key = self.scheduler_key
        now = time.time()
        with self.connection._pipeline() as p:
            p.delete(key)
            p.hset(key, 'birth', now)
            # Set scheduler key to expire a few seconds after polling interval
            # This way, the key will automatically expire if scheduler
            # quits unexpectedly
            p.expire(key, self._interval + 10)
            p.execute()

    def register_death(self):
        """Registers its own death."""
        with self.connection._pipeline() as p:
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

    def _create_job(self, func, args=None, kwargs=None, commit=True,
                    result_ttl=None):
        """
        Creates an RQ job and saves it to Redis.
        """
        if func.__module__ == '__main__':
            raise ValueError(
                    'Functions from the __main__ module cannot be processed '
                    'by workers.')
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        job = Job.create(func, args=args, connection=self.connection,
                         kwargs=kwargs, result_ttl=result_ttl)
        job.origin = self.queue_name
        if commit:
            job.save()
        return job

    def enqueue_at(self, scheduled_time, func, *args, **kwargs):
        """
        Pushes a job to the scheduler queue. The scheduled queue is a Redis sorted
        set ordered by timestamp - which in this case is job's scheduled execution time.

        Usage:

        from datetime import datetime
        from redis import StrictRedis
        from rq.scheduler import Scheduler

        from foo import func

        redis = StrictRedis()
        scheduler = Scheduler(queue_name='default', connection=redis)
        scheduler.enqueue_at(datetime(2020, 1, 1), func, 'argument', keyword='argument')
        """
        job = self._create_job(func, args=args, kwargs=kwargs)
        self.connection._zadd(self.scheduled_jobs_key,
                              times.to_unix(scheduled_time),
                              job.id)
        return job

    def enqueue_in(self, time_delta, func, *args, **kwargs):
        """
        Similar to ``enqueue_at``, but accepts a timedelta instead of datetime object.
        The job's scheduled execution time will be calculated by adding the timedelta
        to times.now().
        """
        job = self._create_job(func, args=args, kwargs=kwargs)
        self.connection._zadd(self.scheduled_jobs_key,
                              times.to_unix(times.now() + time_delta),
                              job.id)
        return job

    def schedule(self, scheduled_time, func, args=None, kwargs=None,
                interval=None, repeat=None, result_ttl=None):
        """
        Schedule a job to be periodically executed, at a certain interval.
        """
        # Set result_ttl to -1 for periodic jobs, if result_ttl not specified
        if interval is not None and result_ttl is None:
            result_ttl = -1
        job = self._create_job(func, args=args, kwargs=kwargs, commit=False,
                               result_ttl=result_ttl)
        if interval is not None:
            job.meta['interval'] = int(interval)
        if repeat is not None:
            job.meta['repeat'] = int(repeat)
        if repeat and interval is None:
            raise ValueError("Can't repeat a job without interval argument")
        job.save()
        self.connection._zadd(self.scheduled_jobs_key,
                              times.to_unix(scheduled_time),
                              job.id)
        return job

    def cancel(self, job):
        """
        Pulls a job from the scheduler queue. This function accepts either a
        job_id or a job instance.
        """
        if isinstance(job, basestring):
            self.connection.zrem(self.scheduled_jobs_key, job)
        else:
            self.connection.zrem(self.scheduled_jobs_key, job.id)

    def __contains__(self, item):
        """
        Returns a boolean indicating whether the given job instance or job id is
        scheduled for execution.
        """
        job_id = item
        if isinstance(item, Job):
            job_id = item.id
        return self.connection.zscore(self.scheduled_jobs_key, job_id) is not None

    def change_execution_time(self, job, date_time):
        """
        Change a job's execution time. Wrap this in a transaction to prevent race condition.
        """
        with self.connection._pipeline() as pipe:
            while 1:
                try:
                    pipe.watch(self.scheduled_jobs_key)
                    if pipe.zscore(self.scheduled_jobs_key, job.id) is None:
                        raise ValueError('Job not in scheduled jobs queue')
                    pipe.zadd(self.scheduled_jobs_key, times.to_unix(date_time), job.id)
                    break
                except WatchError:
                    # If job is still in the queue, retry otherwise job is already executed
                    # so we raise an error
                    if pipe.zscore(self.scheduled_jobs_key, job.id) is None:
                        raise ValueError('Job not in scheduled jobs queue')
                    continue

    def get_jobs(self, until=None, with_times=False):
        """
        Returns a list of job instances that will be queued until the given time.
        If no 'until' argument is given all jobs are returned. This function
        accepts datetime and timedelta instances as well as integers representing
        epoch values.
        If with_times is True a list of tuples consisting of the job instance and
        it's scheduled execution time is returned.
        """
        def epoch_to_datetime(epoch):
            return datetime.fromtimestamp(float(epoch))

        if until is None:
            until = "+inf"
        elif isinstance(until, datetime):
            until = times.to_unix(until)
        elif isinstance(until, timedelta):
            until = times.to_unix((times.now() + until))
        job_ids = self.connection.zrangebyscore(self.scheduled_jobs_key, 0,
                                                until, withscores=with_times,
                                                score_cast_func=epoch_to_datetime)
        if not with_times:
            job_ids = zip(job_ids, repeat(None))
        jobs = []
        for job_id, sched_time in job_ids:
            try:
                job = Job.fetch(job_id, connection=self.connection)
                if with_times:
                    jobs.append((job, sched_time))
                else:
                    jobs.append(job)
            except NoSuchJobError:
                # Delete jobs that aren't there from scheduler
                self.cancel(job_id)
        return jobs

    def get_jobs_to_queue(self, with_times=False):
        """
        Returns a list of job instances that should be queued
        (score lower than current timestamp).
        If with_times is True a list of tuples consisting of the job instance and
        it's scheduled execution time is returned.
        """
        return self.get_jobs(times.to_unix(times.now()), with_times=with_times)

    def get_queue_for_job(self, job):
        """
        Returns a queue to put job into.
        """
        key = '{0}{1}'.format(Queue.redis_queue_namespace_prefix, job.origin)
        return Queue.from_queue_key(key, connection=self.connection)

    def enqueue_job(self, job):
        """
        Move a scheduled job to a queue. In addition, it also does puts the job
        back into the scheduler if needed.
        """
        self.log.debug('Pushing {0} to {1}'.format(job.id, job.origin))

        interval = job.meta.get('interval', None)
        repeat = job.meta.get('repeat', None)

        # If job is a repeated job, decrement counter
        if repeat:
            job.meta['repeat'] = int(repeat) - 1
        job.enqueued_at = times.now()
        job.save()

        queue = self.get_queue_for_job(job)
        queue.push_job_id(job.id)
        self.connection.zrem(self.scheduled_jobs_key, job.id)

        if interval:
            # If this is a repeat job and counter has reached 0, don't repeat
            if repeat is not None:
                if job.meta['repeat'] == 0:
                    return
            self.connection._zadd(self.scheduled_jobs_key,
                                  times.to_unix(times.now()) + int(interval),
                                  job.id)

    def enqueue_jobs(self):
        """
        Move scheduled jobs into queues.
        """
        jobs = self.get_jobs_to_queue()
        for job in jobs:
            self.enqueue_job(job)
        # Refresh scheduler key's expiry
        self.connection.expire(self.scheduler_key, self._interval + 10)
        return jobs

    def run(self, burst=False):
        """
        Periodically check whether there's any job that should queued (score
        lower than current time). If ``burst`` argument is set to True,
        scheduler would run ``enqueue_jobs`` once and quits.
        """
        self.log.debug('Running RQ scheduler...')
        self.register_birth()
        self._install_signal_handlers()
        try:
            while True:
                self.enqueue_jobs()
                if burst:
                    break
                time.sleep(self._interval)
        finally:
            self.register_death()
