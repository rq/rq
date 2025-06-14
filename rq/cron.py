import importlib.util
import logging
import os
import sys
import time
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
        job = queue.enqueue(self.func, *self.args, **self.kwargs, **self.job_options)
        logging.getLogger(__name__).info(f'Enqueued job {self.func.__name__} to queue {self.queue_name}')

        return job

    def get_next_run_time(self) -> datetime:
        """Calculate the next run time based on the current time and interval"""
        if self.interval is None:
            return datetime.max  # Far future if no interval set
        assert self.latest_run_time
        return self.latest_run_time + timedelta(seconds=self.interval)

    def should_run(self) -> bool:
        """Check if this job should run now"""
        if self.latest_run_time is None:
            return True
        if self.next_run_time:
            return now() >= self.next_run_time
        return False

    def set_run_time(self, time: datetime) -> None:
        """Set latest run time to a given time and update next run time"""
        self.latest_run_time = time

        # Update next run time if interval is set
        if self.interval is not None:
            self.next_run_time = self.get_next_run_time()


class CronScheduler:
    """Simple interval-based job scheduler for RQ"""

    def __init__(
        self,
        connection: Redis,
        logging_level: Union[str, int] = logging.INFO,
    ):
        self.connection: Redis = connection
        self._cron_jobs: List[CronJob] = []

        self.log: logging.Logger = logging.getLogger(__name__)
        if not self.log.hasHandlers():
            setup_loghandlers(
                level=logging_level,
                name=__name__,
                log_format=DEFAULT_LOGGING_FORMAT,
                date_format=DEFAULT_LOGGING_DATE_FORMAT,
            )
            self.log.propagate = False

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

        job_key = f'{func.__module__}.{func.__name__}'
        if interval is not None:
            self.log.info(f"Registered '{job_key}' to run on {queue_name} every {interval} seconds")

        return cron_job

    def get_jobs(self) -> List[CronJob]:
        """Get all registered cron jobs"""
        return self._cron_jobs

    def enqueue_jobs(self) -> List[CronJob]:
        """Enqueue all jobs that are due to run"""
        enqueue_time = now()
        enqueued_jobs: List[CronJob] = []
        for job in self._cron_jobs:
            if job.should_run():
                job.enqueue(self.connection)
                job.set_run_time(enqueue_time)
                enqueued_jobs.append(job)
        return enqueued_jobs

    def calculate_sleep_interval(self) -> float:
        """Calculate how long to sleep until the next job is due.

        Returns the number of seconds to sleep, with a maximum of 60 seconds
        to ensure we check regularly.
        """
        current_time = now()

        # Find the next job to run
        next_job_times = [job.next_run_time for job in self._cron_jobs if job.next_run_time]

        if not next_job_times:
            return 60  # Default sleep time of 60 seconds

        # Find the closest job by next_run_time
        closest_time = min(next_job_times)

        # Calculate seconds until next job
        seconds_until_next = (closest_time - current_time).total_seconds()

        # If negative or zero, the job is overdue, so run immediately
        if seconds_until_next <= 0:
            return 0

        # Cap maximum sleep time at 60 seconds
        return min(seconds_until_next, 60)

    def start(self):
        """Start the cron scheduler"""
        self.log.info('Starting cron scheduler...')
        while True:
            self.enqueue_jobs()
            sleep_time = self.calculate_sleep_interval()
            if sleep_time > 0:
                self.log.debug(f'Sleeping for {sleep_time} seconds...')
                time.sleep(sleep_time)

    def load_config_from_file(self, config_path: str):
        """
        Dynamically load a cron config file and register all jobs with this Cron instance.

        Supports both dotted import paths (e.g. 'app.cron_config') and file paths
        (e.g. '/path/to/app/cron_config.py', 'app/cron_config.py'). The .py
        extension is recommended for file paths for clarity.

        Jobs defined in the config file must use the global `rq.cron.register` function.

        Args:
            config_path: Path to the cron_config.py file or module path.
        """
        self.log.info(f'Loading cron configuration from {config_path}')

        global _job_data_registry
        _job_data_registry = []  # Clear global registry before loading module

        is_file_path = os.path.sep in config_path or config_path.endswith('.py')

        if is_file_path:
            # --- Handle as a file path ---
            abs_path = os.path.abspath(config_path)
            self.log.debug(f'Attempting to load as file path: {abs_path}')

            # Immediately check if file exists
            if not os.path.exists(abs_path):
                error_msg = f"Configuration file not found at '{abs_path}'"
                self.log.error(error_msg)
                raise FileNotFoundError(error_msg)

            # Check if it's actually a file and not a directory
            if not os.path.isfile(abs_path):
                error_msg = f"Configuration path points to a directory, not a file: '{abs_path}'"
                self.log.error(error_msg)
                raise IsADirectoryError(error_msg)

            # Load the file as a module
            module_name = f'rq_cron_config_{os.path.basename(config_path).replace(".", "_")}'
            try:
                spec = importlib.util.spec_from_file_location(module_name, abs_path)
                if spec is None or spec.loader is None:
                    error_msg = f'Could not create module spec for {abs_path}'
                    self.log.error(error_msg)
                    raise ImportError(error_msg)

                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                self.log.debug(f'Successfully loaded config from file: {abs_path}')
            except Exception as e:
                if module_name in sys.modules:
                    del sys.modules[module_name]
                error_msg = f"Failed to load configuration file '{abs_path}': {e}"
                self.log.error(error_msg)
                raise ImportError(error_msg) from e

        else:
            # --- Handle as a module path ---
            self.log.debug(f'Attempting to load as module path: {config_path}')
            try:
                importlib.import_module(config_path)
                self.log.debug(f'Successfully loaded config from module: {config_path}')
            except ImportError as e:
                error_msg = f"Failed to import configuration module '{config_path}': {e}"
                self.log.error(error_msg)
                raise ImportError(error_msg) from e
            except Exception as e:
                error_msg = f"An error occurred while importing configuration module '{config_path}': {e}"
                self.log.error(error_msg)
                raise Exception(error_msg) from e

        # Now that the module has been loaded (which populated _job_data_registry
        # via the global `register` function), register the jobs with *this* instance.
        job_count = 0

        for data in _job_data_registry:
            self.log.debug(f'Registering job from config: {data["func"].__name__}')
            try:
                self.register(**data)  # Calls the instance's register method
                job_count += 1
            except Exception as e:
                self.log.error(f'Failed to register job {data["func"].__name__} from config: {e}', exc_info=True)
                # Decide if loading should fail entirely or just skip the job
                # For now, log the error and continue

        # Clear the global registry after we're done
        _job_data_registry = []  # type: ignore
        self.log.info(f"Successfully registered {job_count} cron jobs from '{config_path}'")
        # Method modifies the instance, no need to return self unless chaining is desired


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
    Register a function to be run as a cron job by adding its definition
    to a temporary global registry.

    This function should typically be called from within a cron configuration file
    that will be loaded using `CronScheduler.load_config_from_file()`.

    Example (in your cron_config.py):
        from rq import cron
        from my_app.tasks import my_func

        cron.register(my_func, 'default', interval=60)  # Run every 60 seconds

    Returns:
        dict: The job data dictionary added to the registry.
    """
    # Store the job data in the global registry
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
    # Add to the global registry
    _job_data_registry.append(job_data)

    # Log the registration attempt (optional)
    logger = logging.getLogger(__name__)
    job_key = f'{func.__module__}.{func.__name__}'
    logger.debug(f"Cron config: Adding job '{job_key}' to registry for queue {queue_name}")

    return job_data


def create_cron(connection: Redis) -> CronScheduler:
    """Create a CronScheduler instance with all registered jobs"""
    cron_instance = CronScheduler(connection=connection)

    # Register all previously registered jobs with the CronScheduler instance
    for data in _job_data_registry:
        logging.debug(f'Registering job: {data["func"].__name__}')
        cron_instance.register(**data)

    return cron_instance
