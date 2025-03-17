import logging
import os
import signal
import time
import traceback
import datetime as dt
from datetime import datetime
from enum import Enum
from multiprocessing import Process
from typing import Any, Callable, Iterable, List, Optional, Set, Union

from redis import ConnectionPool, Redis

from rq.types import FunctionReferenceType, JobDependencyType

from .connections import parse_connection
from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT, DEFAULT_SCHEDULER_FALLBACK_PERIOD
from .job import Callback, Job
from .logutils import setup_loghandlers
from .queue import Queue
from .registry import ScheduledJobRegistry
from .serializers import resolve_serializer
from .utils import current_timestamp, parse_names, get_next_scheduled_time, to_unix

SCHEDULER_KEY_TEMPLATE = 'rq:scheduler:%s'
SCHEDULER_LOCKING_KEY_TEMPLATE = 'rq:scheduler-lock:%s'


class SchedulerStatus(str, Enum):
    STARTED = 'started'
    """Scheduler has been started but sleeping"""
    WORKING = 'working'
    """Scheduler is in the midst of scheduling jobs"""
    STOPPED = 'stopped'
    """Scheduler is in stopped condition"""


class RQScheduler:
    Status = SchedulerStatus

    def __init__(
        self,
        queues,
        connection: Redis,
        interval=1,
        logging_level: Union[str, int] = logging.INFO,
        date_format=DEFAULT_LOGGING_DATE_FORMAT,
        log_format=DEFAULT_LOGGING_FORMAT,
        serializer=None,
    ):
        self._queue_names = set(parse_names(queues))
        self._acquired_locks: Set[str] = set()
        self._scheduled_job_registries: List[ScheduledJobRegistry] = []
        self.lock_acquisition_time = None
        self._connection_class, self._pool_class, self._pool_kwargs = parse_connection(connection)
        self.serializer = resolve_serializer(serializer)

        self._connection = None
        self.interval = interval
        self._stop_requested = False
        self._status = self.Status.STOPPED
        self._process = None
        self.log = logging.getLogger(__name__)
        setup_loghandlers(
            level=logging_level,
            name=__name__,
            log_format=log_format,
            date_format=date_format,
        )

    @property
    def connection(self):
        if self._connection:
            return self._connection
        self._connection = self._connection_class(
            connection_pool=ConnectionPool(connection_class=self._pool_class, **self._pool_kwargs)
        )
        return self._connection

    @property
    def acquired_locks(self):
        return self._acquired_locks

    @property
    def status(self):
        return self._status

    @property
    def should_reacquire_locks(self):
        """Returns True if lock_acquisition_time is longer than 10 minutes ago"""
        if self._queue_names == self.acquired_locks:
            return False
        if not self.lock_acquisition_time:
            return True
        return (datetime.now() - self.lock_acquisition_time).total_seconds() > DEFAULT_SCHEDULER_FALLBACK_PERIOD

    def acquire_locks(self, auto_start=False):
        """Returns names of queue it successfully acquires lock on"""
        successful_locks = set()
        pid = os.getpid()
        self.log.debug('Trying to acquire locks for %s', ', '.join(self._queue_names))
        for name in self._queue_names:
            if self.connection.set(self.get_locking_key(name), pid, nx=True, ex=self.interval + 60):
                successful_locks.add(name)

        # Always reset _scheduled_job_registries when acquiring locks
        self._scheduled_job_registries = []
        self._acquired_locks = self._acquired_locks.union(successful_locks)
        self.lock_acquisition_time = datetime.now()

        # If auto_start is requested and scheduler is not started,
        # run self.start()
        if self._acquired_locks and auto_start:
            if not self._process or not self._process.is_alive():
                self.start()

        return successful_locks

    def prepare_registries(self, queue_names: Optional[Iterable[str]] = None):
        """Prepare scheduled job registries for use"""
        self._scheduled_job_registries = []
        if not queue_names:
            queue_names = self._acquired_locks
        for name in queue_names:
            self._scheduled_job_registries.append(
                ScheduledJobRegistry(name, connection=self.connection, serializer=self.serializer)
            )

    @classmethod
    def get_locking_key(cls, name: str):
        """Returns scheduler key for a given queue name"""
        return SCHEDULER_LOCKING_KEY_TEMPLATE % name

    def cron(
        self,
        cron_string: str,
        func: FunctionReferenceType,
        args: Optional[List] = None,
        kwargs: Optional[dict] = None,
        repeat: Optional[int] = None,
        queue_name: Optional[str] = None,
        result_ttl: int = -1,
        ttl: Optional[int] = None,
        job_id: Optional[str] = None,
        timeout: Optional[int] = None,
        description: Optional[str] = None,
        meta: Optional[dict] = None,
        use_local_timezone: bool = False,
        depends_on: Optional[JobDependencyType] = None,
        on_success: Optional[Union['Callback', Callable[..., Any]]] = None,
        on_failure: Optional[Union['Callback', Callable[..., Any]]] = None,
        at_front: bool = False,
    ) -> Job:
        """
        Schedule a job to be run periodically based on a cron string.
        
        This method requires the croniter package. If it's not installed,
        a warning will be issued and the job will be scheduled to run once
        after a short delay.
        
        Args:
            cron_string (str): Cron expression (e.g. "0 0 * * *")
            func (callable): Function to execute
            args (list, optional): Arguments to pass to the function
            kwargs (dict, optional): Keyword arguments to pass to the function
            repeat (int, optional): Number of times to repeat the job
            queue_name (str, optional): Queue to place the job in
            result_ttl (int, optional): Time to retain the job result
            ttl (int, optional): Maximum queued time before the job is discarded
            job_id (str, optional): Custom ID for the job
            timeout (int, optional): Maximum job execution time
            description (str, optional): Description of the job
            meta (dict, optional): Additional metadata for the job
            use_local_timezone (bool, optional): Whether to use local timezone. Defaults to False.
            depends_on (str, optional): Job ID that this job depends on
            on_success (callable, optional): Function to call on success
            on_failure (callable, optional): Function to call on failure
            at_front (bool, optional): Whether to place the job at the front of the queue
            
        Returns:
            Job: The scheduled job instance
        """
        scheduled_time = get_next_scheduled_time(cron_string, use_local_timezone=use_local_timezone)
        self.log.debug(f"Scheduling job with cron string: {cron_string}, scheduled time: {scheduled_time}")
        
        job = Job.create(
            func,
            args=args or (),
            kwargs=kwargs or {},
            connection=self.connection,
            result_ttl=result_ttl,
            ttl=ttl,
            id=job_id,
            description=description,
            timeout=timeout,
            meta=meta or {},
            depends_on=depends_on,
            on_success=on_success,
            on_failure=on_failure,
            serializer=self.serializer,
        )
        
        if queue_name:
            job.origin = queue_name
        else:
            job.origin = next(iter(self._queue_names)) if self._queue_names else 'default'
        
        job.meta['cron_string'] = cron_string
        job.meta['use_local_timezone'] = use_local_timezone

        if repeat is not None:
            job.meta['repeat'] = int(repeat)
        
        if at_front:
            job.enqueue_at_front = True
        
        job.save()
        registry = ScheduledJobRegistry(job.origin, connection=self.connection, serializer=self.serializer)
        registry.schedule(job, scheduled_time)
        
        return job

    def enqueue_scheduled_jobs(self):
        """
        Enqueue jobs whose timestamp is in the past and reschedule recurring jobs.
        
        This method:
        1. Fetches jobs that are due to be executed
        2. Enqueues them in their respective queues
        3. Reschedules recurring jobs (cron jobs or interval jobs)
        """
        self._status = self.Status.WORKING
        self.log.debug("Checking for scheduled jobs")

        if not self._scheduled_job_registries and self._acquired_locks:
            self.prepare_registries()

        for registry in self._scheduled_job_registries:
            try:
                timestamp = current_timestamp()
                job_ids = registry.get_jobs_to_schedule(timestamp)
                
                if not job_ids:
                    continue
                    
                queue = Queue(registry.name, connection=self.connection, serializer=self.serializer)
                jobs = Job.fetch_many(job_ids, connection=self.connection, serializer=self.serializer)
                valid_jobs = [job for job in jobs if job is not None]

                with self.connection.pipeline() as pipeline:
                    for job in valid_jobs:
                        queue._enqueue_job(job, pipeline=pipeline, at_front=bool(job.enqueue_at_front))
                        self._reschedule_recurring_job(job, registry, pipeline)
                    
                    for job_id in job_ids:
                        registry.remove(job_id, pipeline=pipeline)
                    
                    pipeline.execute()
                    
                self.log.debug(f"Enqueued {len(valid_jobs)} jobs from {registry.name} queue")
                
            except Exception as e:
                self.log.error(f"Error processing scheduled jobs for {registry.name}: {str(e)}")
                self.log.debug(traceback.format_exc())
        
        self._status = self.Status.STARTED

    def _reschedule_recurring_job(self, job: Optional['Job'], registry: 'ScheduledJobRegistry', pipeline=None):
        """
        Reschedule a recurring job (cron or interval) if needed.
        
        Args:
            job (Job): The job to potentially reschedule
            registry (ScheduledJobRegistry): The registry to schedule the job in
            pipeline (Pipeline, optional): Redis pipeline to use. Defaults to None.
        
        Returns:
            bool: True if job was rescheduled, False otherwise
        """
        if job is None:
            return False
            
        execute_pipeline = False
        if pipeline is None:
            pipeline = self.connection.pipeline()
            execute_pipeline = True
        
        rescheduled = False
        
        try:
            if 'cron_string' in job.meta:
                cron_string = job.meta['cron_string']
                use_local_timezone = job.meta.get('use_local_timezone', False)
                repeat = job.meta.get('repeat')
                
                if repeat is not None:
                    job.meta['repeat'] = int(repeat) - 1
                    job.save(pipeline=pipeline)
                    if job.meta['repeat'] < 0:
                        return False
                
                next_scheduled_time = get_next_scheduled_time(cron_string, use_local_timezone=use_local_timezone)
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)
                rescheduled = True
                
            elif 'interval' in job.meta:
                interval = int(job.meta['interval'])
                repeat = job.meta.get('repeat')
                
                if repeat is not None:
                    job.meta['repeat'] = int(repeat) - 1
                    job.save(pipeline=pipeline)
                    
                    if job.meta['repeat'] < 0:
                        return False
                
                next_scheduled_time = datetime.now(dt.UTC) + dt.timedelta(seconds=interval)
                registry.schedule(job, next_scheduled_time, pipeline=pipeline)
                rescheduled = True
        
        except Exception as e:
            self.log.error(f"Error rescheduling job {job.id}: {str(e)}")
            self.log.debug(traceback.format_exc())
        
        if execute_pipeline:
            pipeline.execute()
        
        return rescheduled

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
        self.log.debug('Scheduler sending heartbeat to %s', ', '.join(self.acquired_locks))
        if len(self._acquired_locks) > 1:
            with self.connection.pipeline() as pipeline:
                for name in self._acquired_locks:
                    key = self.get_locking_key(name)
                    pipeline.expire(key, self.interval + 60)
                pipeline.execute()
        elif self._acquired_locks:
            key = self.get_locking_key(next(iter(self._acquired_locks)))
            self.connection.expire(key, self.interval + 60)

    def stop(self):
        self.log.info('Scheduler stopping, releasing locks for %s...', ', '.join(self._acquired_locks))
        self.release_locks()
        self._status = self.Status.STOPPED

    def release_locks(self):
        """Release acquired locks"""
        keys = [self.get_locking_key(name) for name in self._acquired_locks]
        self.connection.delete(*keys)
        self._acquired_locks = set()

    def start(self):
        self._status = self.Status.STARTED
        # Redis instance can't be pickled across processes so we need to
        # clean this up before forking
        self._connection = None
        self._process = Process(target=run, args=(self,), name='Scheduler')
        self._process.start()
        return self._process

    def work(self):
        self._install_signal_handlers()

        while True:
            if self._stop_requested:
                self.stop()
                break

            if self.should_reacquire_locks:
                self.acquire_locks()

            self.enqueue_scheduled_jobs()
            self.heartbeat()
            time.sleep(self.interval)


def run(scheduler):
    scheduler.log.info('Scheduler for %s started with PID %s', ', '.join(scheduler._queue_names), os.getpid())
    try:
        scheduler.work()
    except:  # noqa
        scheduler.log.error('Scheduler [PID %s] raised an exception.\n%s', os.getpid(), traceback.format_exc())
        raise
    scheduler.log.info('Scheduler with PID %d has stopped', os.getpid())
