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
        job = queue.enqueue(
            self.func,
            *self.args,
            **self.kwargs,
            **self.job_options
        )
        logging.getLogger(__name__).info(
            f"Enqueued job {self.func.__name__} to queue {self.queue_name}"
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

        job_key = f"{func.__module__}.{func.__name__}"
        if interval is not None:
            self.log.info(f"Registered cron job '{job_key}' to run on {queue_name} every {interval} seconds")
        else:
            self.log.info(f"Registered cron job '{job_key}' for manual execution only")

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
        self.log.info("Starting cron scheduler...")
        while True:
            self.enqueue_jobs()
            sleep_time = self.calculate_sleep_interval()
            if sleep_time > 0:
                self.log.info(f"Sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)


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
    (e.g. '/path/to/app/cron_config.py', 'app/cron_config.py'). The .py
    extension is recommended for file paths for clarity.

    Args:
        config_path: Path to the cron_config.py file or module path.
        connection: Redis connection to use for the Cron instance

    Returns:
        Cron instance with all jobs registered
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Loading cron configuration from {config_path}")

    global _job_data_registry
    _job_data_registry = []  # Clear registry before loading

    is_file_path = os.path.sep in config_path or config_path.endswith('.py')
    module_loaded = False
    error = None

    if is_file_path:
        # --- Handle as a file path ---
        abs_path = os.path.abspath(config_path)
        logger.debug(f"Attempting to load as file path: {abs_path}")
        if not os.path.exists(abs_path):
            error = FileNotFoundError(f"Configuration file not found at '{abs_path}'")
        elif not os.path.isfile(abs_path):
             error = IsADirectoryError(f"Configuration path points to a directory, not a file: '{abs_path}'")
        else:
            # Derive a unique module name to avoid conflicts if loading multiple files
            # Or use a fixed name if you prefer, but be aware of potential sys.modules clashes
            module_name = f"rq_cron_config_{os.path.basename(config_path).replace('.', '_')}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, abs_path)
                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not create module spec for {abs_path}")

                module = importlib.util.module_from_spec(spec)
                # Important: Add to sys.modules *before* exec_module to handle potential
                # relative imports within the config file itself.
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                module_loaded = True
                logger.debug(f"Successfully loaded config from file: {abs_path}")
            except Exception as e:
                # Clean up from sys.modules if loading failed
                if module_name in sys.modules:
                    del sys.modules[module_name]
                error = ImportError(f"Failed to load configuration file '{abs_path}': {e}")

    else:
        # --- Handle as a module path ---
        logger.debug(f"Attempting to load as module path: {config_path}")
        try:
            # Check if already loaded (e.g., by a previous import somewhere else)
            if config_path in sys.modules:
                 logger.warning(f"Module '{config_path}' already loaded. Re-importing might not re-execute registration if module hasn't changed.")
                 # You might want importlib.reload here if re-execution is desired,
                 # but be cautious about side effects. Standard import is usually safer.
            importlib.import_module(config_path)
            module_loaded = True
            logger.debug(f"Successfully loaded config from module: {config_path}")
        except ImportError as e:
            error = ImportError(f"Failed to import configuration module '{config_path}': {e}")
        except Exception as e: # Catch other potential errors during import
             error = Exception(f"An error occurred while importing configuration module '{config_path}': {e}")


    if not module_loaded or error:
        # Raise the specific error encountered
        final_error = error or RuntimeError("Unknown error occurred during configuration loading.")
        logger.error(f"Failed to load cron configuration from '{config_path}': {final_error}")
        raise final_error

    # Now that the module has been loaded and executed exactly once,
    # create the Cron instance using the populated registry.
    cron_instance = create_cron(connection)

    logger.info(f"Successfully registered {len(cron_instance.get_jobs())} cron jobs from '{config_path}'")

    return cron_instance
