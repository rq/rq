import importlib.util
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from redis import Redis

from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue
from .utils import now


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
        self.next_run_time: Optional[datetime] = None
        self.latest_run_time: Optional[datetime] = None
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
        logging.getLogger(__name__).info(
            f"Enqueued job {self.func.__name__}to queue {self.queue_name}"
        )

        return job

    def get_next_run_time(self) -> datetime:
        """Calculate the next run time based on the current time and interval"""
        if self.interval is None:
            return datetime.max  # Far future if no interval set
        assert self.latest_run_time
        return self.latest_run_time + timedelta(seconds=self.interval)

    def should_run(self) -> bool:
        """Check if this job should run now"""
        if self.next_run_time:
            return now() >= self.next_run_time
        return False

    def set_run_time(self, time: datetime) -> None:
        """Set latest run time to a given time and update next run time"""
        self.latest_run_time = time

        # Update next run time if interval is set
        if self.interval is not None:
            self.next_run_time = self.get_next_run_time()


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
            self.log.info(f"Registered cron job '{job_key}' to run on {queue_name} every {interval} seconds")
        else:
            self.log.info(f"Registered cron job '{job_key}' for manual execution only")

        return cron_job

    def get_jobs(self) -> List[CronJob]:
        """Get all registered cron jobs"""
        return self._cron_jobs

    def enqueue_jobs(self) -> None:
        """Enqueue all jobs that are due to run"""
        enqueue_time = now()
        for job in self._cron_jobs:
            if job.should_run():
                job.enqueue(self.connection)
                job.set_run_time(enqueue_time)

    def calculate_sleep_interval(self) -> float:
        """Calculate how long to sleep until the next job is due.

        Returns the number of seconds to sleep.
        """
        return 1


# Global registry to store job data before Cron instance is created
_job_data_registry: List[Dict] = []


def register(
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
    for data in _job_data_registry:
        logging.debug(f"Registering job: {data['func'].__name__}")
        cron_instance.register(**data)

    return cron_instance


def load_config(config_path: str, connection: Redis) -> Cron:
    """
    Dynamically load a cron config file and register all jobs with a Cron instance.

    Supports both dotted import paths (e.g. 'app.cron_config') and file paths
    (e.g. 'app/cron_config.py'). The .py extension is optional for file paths.

    Args:
        config_path: Path to the cron_config.py file
        connection: Redis connection to use for the Cron instance

    Returns:
        Cron instance with all jobs registered
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Loading cron configuration from {config_path}")

    global _job_data_registry
    _job_data_registry = []

    # Track all attempted paths for better error reporting
    attempted_paths = []
    last_error = None

    # Try to interpret config_path as a module path
    try:
        attempted_paths.append(f"Module import: '{config_path}'")
        importlib.import_module(config_path)
        # If we reach here, the import was successful
    except ImportError as e:
        last_error = e

        # Try as a file path with .py extension
        if not config_path.endswith('.py'):
            file_path = f"{config_path}.py"
        else:
            file_path = config_path

        abs_path = os.path.abspath(file_path)
        attempted_paths.append(f"File path: '{abs_path}'")

        if not os.path.exists(abs_path):
            # Try as a file path without .py extension if the original had it
            if config_path.endswith('.py'):
                base_path = config_path[:-3]  # Remove .py
                abs_base_path = os.path.abspath(base_path)
                attempted_paths.append(f"File path without extension: '{abs_base_path}'")
                if os.path.exists(abs_base_path) and os.path.isfile(abs_base_path):
                    abs_path = abs_base_path
                else:
                    error_msg = (
                        f"Could not load cron configuration. Tried the following:\n"
                        f"{chr(10).join('- ' + path for path in attempted_paths)}\n"
                        f"Last error: {str(last_error)}"
                    )
                    raise FileNotFoundError(error_msg)
            else:
                error_msg = (
                    f"Could not load cron configuration. Tried the following:\n"
                    f"{chr(10).join('- ' + path for path in attempted_paths)}\n"
                    f"Last error: {str(last_error)}"
                )
                raise FileNotFoundError(error_msg)

        # Load the module dynamically from file path
        module_name = "cron_config"
        try:
            spec = importlib.util.spec_from_file_location(module_name, abs_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not create module spec for {abs_path}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        except Exception as e:
            attempted_paths.append(f"Dynamic loading of file: '{abs_path}' as module '{module_name}'")
            error_msg = (
                f"Failed to load cron configuration. Tried the following:\n"
                f"{chr(10).join('- ' + path for path in attempted_paths)}\n"
                f"Last error: {str(e)}"
            )
            raise ImportError(error_msg) from e

    # Now that the module has been loaded and executed, all register() calls
    # have populated the _job_data_registry, so we can create the Cron instance
    cron_instance = create_cron(connection)

    logger.info(f"Successfully registered {len(cron_instance.get_jobs())} cron jobs")

    return cron_instance
