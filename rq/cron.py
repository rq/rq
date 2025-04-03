import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar

from redis import Redis

from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue

# Type variable for any callable
F = TypeVar('F', bound=Callable)


class CronJob:
    """Represents a function to be run on a time interval"""

    def __init__(
        self,
        func: Callable,
        queue_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        interval: Optional[int] = None,
        timeout: Optional[int] = None,
        result_ttl: int = 500,
        ttl: Optional[int] = None,
        failure_ttl: Optional[int] = None,
        meta: Optional[dict] = None,
    ):
        self.func: Callable = func
        self.args: Tuple = args or ()
        self.kwargs: Dict = kwargs or {}
        self.interval: Optional[int] = interval
        self.queue_name: str = queue_name
        self.next_run: datetime = datetime.now()  # Start immediately by default
        self.job_options: Dict[str, Any] = {
            'timeout': timeout,
            'result_ttl': result_ttl,
            'ttl': ttl,
            'failure_ttl': failure_ttl,
            'meta': meta,
        }
        # Filter out None values
        self.job_options = {k: v for k, v in self.job_options.items() if v is not None}

    def enqueue(self, connection: Redis) -> Job:
        """Enqueue this job to its queue and update the next run time"""
        queue = Queue(self.queue_name, connection=connection)
        job = queue.enqueue(
            self.func,
            *self.args,
            **self.kwargs,
            **self.job_options
        )

        # Update next run time if interval is set
        if self.interval is not None:
            self.next_run = self.get_next_run_time()

        return job

    def get_next_run_time(self) -> datetime:
        """Calculate the next run time based on the current time and interval"""
        if self.interval is None:
            return datetime.max  # Far future if no interval set
        return datetime.now() + timedelta(seconds=self.interval)

    def should_run(self) -> bool:
        """Check if this job should run now"""
        # If no interval set, job only runs when explicitly triggered
        if self.interval is None:
            return False
        return datetime.now() >= self.next_run


class Cron:
    """Simple interval-based job scheduler for RQ"""

    def __init__(
        self,
        connection: Redis,
        logging_level: Union[str, int] = logging.INFO,
    ):
        self.connection: Redis = connection
        self._cron_jobs: List[CronJob] = []

        self.log: logging.Logger = logging.getLogger(__name__)
        setup_loghandlers(
            level=logging_level,
            name=__name__,
            log_format=DEFAULT_LOGGING_FORMAT,
            date_format=DEFAULT_LOGGING_DATE_FORMAT,
        )

    def register(
        self,
        func: F,
        queue_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        interval: Optional[int] = None,
        timeout: Optional[int] = None,
        result_ttl: int = 500,
        ttl: Optional[int] = None,
        failure_ttl: Optional[int] = None,
        meta: Optional[dict] = None,
    ) -> CronJob:
        """Register a function to be run at regular intervals"""
        cron_job = CronJob(
            func=func,
            queue_name=queue_name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            timeout=timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            meta=meta,
        )

        self._cron_jobs.append(cron_job)

        job_key = f"{func.__module__}.{func.__name__}"
        if interval is not None:
            self.log.info(f"Registered cron job '{job_key}' to run every {interval} seconds")
        else:
            self.log.info(f"Registered cron job '{job_key}' for manual execution only")

        return cron_job

    def get_jobs(self) -> List[CronJob]:
        """Get all registered cron jobs"""
        return self._cron_jobs


# Global registry to store job data before Cron instance is created
_job_data_registry: List[Dict] = []


def register(
    func: F,
    queue_name: str = "default",
    args: Optional[Tuple] = None,
    kwargs: Optional[Dict] = None,
    interval: Optional[int] = None,
    timeout: Optional[int] = None,
    result_ttl: int = 500,
    ttl: Optional[int] = None,
    failure_ttl: Optional[int] = None,
    meta: Optional[dict] = None,
) -> Dict:
    """
    Register a function to be run as a cron job.

    Example:
        from rq import cron

        cron_job = cron.register(my_func, interval=60)  # Run every 60 seconds

    Returns:
        CronJob: The created CronJob object
    """
    # Store the job data in the registry
    job_data = {
        'func': func,
        'queue_name': queue_name,
        'args': args,
        'kwargs': kwargs,
        'interval': interval,
        'timeout': timeout,
        'result_ttl': result_ttl,
        'ttl': ttl,
        'failure_ttl': failure_ttl,
        'meta': meta,
    }

    # Add to the registry
    _job_data_registry.append(job_data)

    # Create and return a CronJob instance for immediate use if needed
    return job_data


def create_cron(connection: Redis) -> Cron:
    """Create a Cron instance with all registered jobs"""
    cron_instance = Cron(connection=connection)

    # Register all previously registered jobs with the Cron instance
    for job_data in _job_data_registry:
        cron_instance.register(**job_data)

    return cron_instance
