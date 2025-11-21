import importlib.util
import logging
import os
import signal
import socket
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from croniter import croniter
from redis import Redis
from redis.client import Pipeline

from . import cron_scheduler_registry
from .defaults import DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT
from .exceptions import SchedulerNotFound, StopRequested
from .job import Job
from .logutils import setup_loghandlers
from .queue import Queue
from .serializers import resolve_serializer
from .utils import decode_redis_hash, normalize_config_path, now, str_to_date, utcformat, validate_absolute_path


class CronJob:
    """Represents a function to be run on a time interval"""

    def __init__(
        self,
        queue_name: str,
        func: Optional[Callable] = None,
        func_name: Optional[str] = None,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        interval: Optional[int] = None,
        cron: Optional[str] = None,
        job_timeout: Optional[int] = None,
        result_ttl: int = 500,
        ttl: Optional[int] = None,
        failure_ttl: Optional[int] = None,
        meta: Optional[dict] = None,
    ):
        if interval and cron:
            raise ValueError('Cannot specify both interval and cron parameters')
        if not interval and not cron:
            raise ValueError('Must specify either interval or cron parameter')

        if func:
            self.func: Optional[Callable] = func
            self.func_name: str = f'{func.__module__}.{func.__name__}'
        elif func_name:
            self.func = None
            self.func_name = func_name
        else:
            raise ValueError('Either func or func_name must be provided')

        self.args: Tuple = args or ()
        self.kwargs: Dict = kwargs or {}
        self.interval: Optional[int] = interval
        self.cron: Optional[str] = cron
        self.queue_name: str = queue_name
        self.next_run_time: Optional[datetime] = None
        self.latest_run_time: Optional[datetime] = None

        # For cron jobs, set initial next_run_time during initialization
        if self.cron:
            cron_iter = croniter(self.cron, now())
            self.next_run_time = cron_iter.get_next(datetime)
        self.job_options: Dict[str, Any] = {
            'job_timeout': job_timeout,
            'result_ttl': result_ttl,
            'ttl': ttl,
            'failure_ttl': failure_ttl,
            'meta': meta,
        }
        # Filter out None values
        self.job_options = {k: v for k, v in self.job_options.items() if v is not None}

    def enqueue(self, connection: Redis) -> Job:
        """Enqueue this job to its queue and update the next run time"""
        if not self.func:
            raise ValueError('CronJob has no function to enqueue. It may have been created for monitoring purposes.')

        queue = Queue(self.queue_name, connection=connection)
        job = queue.enqueue(self.func, *self.args, **self.kwargs, **self.job_options)
        logging.getLogger(__name__).info(f'Enqueued job {self.func.__name__} to queue {self.queue_name}')

        return job

    def get_next_run_time(self) -> datetime:
        """Calculate the next run time based on interval or cron expression"""
        if self.cron:
            # Use cron expression to calculate next run time
            cron_iter = croniter(self.cron, self.latest_run_time or now())
            return cron_iter.get_next(datetime)
        elif self.interval and self.latest_run_time:
            # Use interval-based calculation
            return self.latest_run_time + timedelta(seconds=self.interval)

        return datetime.max  # Far future if neither interval nor cron set

    def should_run(self) -> bool:
        """Check if this job should run now"""
        # For interval jobs that have never run, run immediately
        # Jobs with cron string always have next_run_time set during initialization
        if not self.latest_run_time and not self.cron:
            return True

        # For all other cases, check if next_run_time has arrived
        if self.next_run_time:
            return now() >= self.next_run_time

        return False

    def set_run_time(self, time: datetime) -> None:
        """Set latest run time to a given time and update next run time"""
        self.latest_run_time = time

        # Update next run time if interval or cron is set
        if self.interval is not None or self.cron is not None:
            self.next_run_time = self.get_next_run_time()

    def to_dict(self) -> Dict[str, Any]:
        """Convert CronJob instance to a dictionary for monitoring purposes"""
        obj = {
            'func_name': self.func_name,
            'queue_name': self.queue_name,
            'interval': self.interval,
            'cron': self.cron,
        }
        # Add job options, filtering out None values
        obj.update({k: v for k, v in self.job_options.items() if v is not None})
        return obj

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CronJob':
        """Create a CronJob instance from dictionary data for monitoring purposes.

        Note: The returned CronJob will not have a func attribute and cannot be executed,
        but contains all the metadata for monitoring.
        """
        return cls(**data)


class CronScheduler:
    """Simple interval-based job scheduler for RQ"""

    def __init__(
        self,
        connection: Redis,
        logging_level: Union[str, int] = logging.INFO,
        name: str = '',
    ):
        self.connection: Redis = connection
        self._cron_jobs: List[CronJob] = []
        self.hostname: str = socket.gethostname()
        self.pid: int = os.getpid()
        self.name: str = name or f'{self.hostname}:{self.pid}:{uuid.uuid4().hex[:6]}'
        self.config_file: str = ''
        self.created_at: datetime = now()
        self.serializer = resolve_serializer()

        self.log: logging.Logger = logging.getLogger(__name__)
        if not self.log.hasHandlers():
            setup_loghandlers(
                level=logging_level,
                name=__name__,
                log_format=DEFAULT_LOGGING_FORMAT,
                date_format=DEFAULT_LOGGING_DATE_FORMAT,
            )
            self.log.propagate = False

    def __eq__(self, other) -> bool:
        """Equality does not take the database/connection into account"""
        if not isinstance(other, self.__class__):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        """The hash does not take the database/connection into account"""
        return hash(self.name)

    def register(
        self,
        func: Callable,
        queue_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        interval: Optional[int] = None,
        cron: Optional[str] = None,
        job_timeout: Optional[int] = None,
        result_ttl: int = 500,
        ttl: Optional[int] = None,
        failure_ttl: Optional[int] = None,
        meta: Optional[dict] = None,
    ) -> CronJob:
        """Register a function to be run at regular intervals"""
        cron_job = CronJob(
            queue_name=queue_name,
            func=func,
            args=args,
            kwargs=kwargs,
            interval=interval,
            cron=cron,
            job_timeout=job_timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            meta=meta,
        )

        self._cron_jobs.append(cron_job)

        job_key = f'{func.__module__}.{func.__name__}'
        if interval:
            self.log.info(f"Registered '{job_key}' to run on {queue_name} every {interval} seconds")
        elif cron:
            self.log.info(f"Registered '{job_key}' to run on {queue_name} with cron schedule '{cron}'")

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

    def _install_signal_handlers(self):
        """Install signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)

    def _request_stop(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.log.info('CronScheduler %s: received shutdown signal %s', self.name, signum)
        raise StopRequested()

    def start(self):
        """Start the cron scheduler"""
        self.log.info('CronScheduler %s: starting...', self.name)

        # Register birth and install signal handlers
        self._install_signal_handlers()
        self.register_birth()

        try:
            while True:
                self.enqueue_jobs()
                self.heartbeat()
                sleep_time = self.calculate_sleep_interval()
                if sleep_time > 0:
                    self.log.debug(f'Sleeping for {sleep_time} seconds...')
                    time.sleep(sleep_time)
        except KeyboardInterrupt:
            self.log.info('CronScheduler %s: received KeyboardInterrupt', self.name)
        except StopRequested:
            self.log.info('CronScheduler %s: stop requested', self.name)
        finally:
            # Register death before shutting down
            self.register_death()
            self.log.info('CronScheduler %s: shutdown complete', self.name)

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
        self.config_file = config_path
        self.log.info(f'Loading cron configuration from {config_path}')

        global _job_data_registry
        _job_data_registry = []  # Clear global registry before loading module

        if os.path.isabs(config_path):
            # Absolute paths must be loaded by file path (cannot be converted to valid module paths)
            self.log.debug(f'Loading absolute file path: {config_path}')

            # Validate the file path
            validate_absolute_path(config_path)

            # Load the file as a module
            module_name = f'rq_cron_config_{os.path.basename(config_path).replace(".", "_")}'
            try:
                spec = importlib.util.spec_from_file_location(module_name, config_path)
                if spec is None or spec.loader is None:
                    error_msg = f'Could not create module spec for {config_path}'
                    self.log.error(error_msg)
                    raise ImportError(error_msg)

                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                self.log.debug(f'Successfully loaded config from file: {config_path}')
            except Exception as e:
                if module_name in sys.modules:
                    del sys.modules[module_name]
                error_msg = f"Failed to load configuration file '{config_path}': {e}"
                self.log.error(error_msg)
                raise ImportError(error_msg) from e
        else:
            # Relative paths and dotted paths - normalize to dotted module format
            normalized_path = normalize_config_path(config_path)
            self.log.debug(f'Normalized path: {normalized_path}')

            # Import the module using the normalized dotted path
            try:
                if normalized_path in sys.modules:
                    importlib.reload(sys.modules[normalized_path])
                else:
                    importlib.import_module(normalized_path)
                self.log.debug(f'Successfully loaded config from module: {normalized_path}')
            except ImportError as e:
                error_msg = f"Failed to import configuration module '{normalized_path}' (from '{config_path}'): {e}"
                self.log.error(error_msg)
                raise ImportError(error_msg) from e
            except Exception as e:
                error_msg = f"An error occurred while importing '{normalized_path}' (from '{config_path}'): {e}"
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

    @property
    def key(self) -> str:
        """Redis key for this CronScheduler instance"""
        return f'rq:cron_scheduler:{self.name}'

    def to_dict(self) -> Dict:
        """Convert CronScheduler instance to a dictionary for Redis storage"""
        obj = {
            'hostname': self.hostname,
            'pid': str(self.pid),
            'name': self.name,
            'created_at': utcformat(self.created_at),
            'config_file': self.config_file or '',
        }
        return obj

    def save(self, pipeline: Optional[Pipeline] = None) -> None:
        """Save CronScheduler instance to Redis hash with TTL"""
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, mapping=self.to_dict())
        connection.expire(self.key, 60)

    def restore(self, raw_data: Dict) -> None:
        """Restore CronScheduler instance from Redis hash data"""
        obj = decode_redis_hash(raw_data, decode_values=True)

        self.hostname = obj['hostname']
        self.pid = int(obj.get('pid', 0))
        self.name = obj['name']
        self.created_at = str_to_date(obj['created_at'])
        self.config_file = obj['config_file']

    @classmethod
    def fetch(cls, name: str, connection: Redis) -> 'CronScheduler':
        """Fetch a CronScheduler instance from Redis by name"""
        key = f'rq:cron_scheduler:{name}'
        raw_data = connection.hgetall(key)

        if not raw_data:
            raise SchedulerNotFound(f"CronScheduler with name '{name}' not found")

        scheduler = cls(connection=connection, name=name)
        scheduler.restore(raw_data)
        return scheduler

    @classmethod
    def all(cls, connection: Redis, cleanup: bool = True) -> List['CronScheduler']:
        """Returns all CronScheduler instances from the registry

        Args:
            connection: Redis connection to use
            cleanup: If True, removes stale entries from registry before fetching schedulers

        Returns:
            List of CronScheduler instances
        """
        from contextlib import suppress

        if cleanup:
            cron_scheduler_registry.cleanup(connection)

        scheduler_names = cron_scheduler_registry.get_keys(connection)
        schedulers = []

        for name in scheduler_names:
            with suppress(SchedulerNotFound):
                scheduler = cls.fetch(name, connection)
                schedulers.append(scheduler)

        return schedulers

    def register_birth(self) -> None:
        """Register this scheduler's birth in the scheduler registry and save data to Redis hash"""
        self.log.info(f'CronScheduler {self.name}: registering birth...')

        with self.connection.pipeline() as pipeline:
            cron_scheduler_registry.register(self, pipeline)
            self.save(pipeline)
            pipeline.execute()

    def register_death(self, pipeline: Optional[Pipeline] = None) -> None:
        """Register this scheduler's death by removing it from the scheduler registry"""
        self.log.info(f'CronScheduler {self.name}: registering death...')
        cron_scheduler_registry.unregister(self, pipeline)

    def heartbeat(self) -> None:
        """Send a heartbeat to update this scheduler's last seen timestamp in the registry
        and extend the scheduler's Redis hash TTL.
        """
        with self.connection.pipeline() as pipe:
            pipe.zadd(cron_scheduler_registry.get_registry_key(), {self.name: time.time()}, xx=True, ch=True)
            pipe.expire(self.key, 120)
            results = pipe.execute()

            # Check zadd result (first command in pipeline)
            zadd_result = results[0]
            if zadd_result:
                self.log.debug(f'CronScheduler {self.name}: heartbeat sent successfully')
            else:
                self.log.warning(f'CronScheduler {self.name}: heartbeat failed - scheduler not found in registry')

    @property
    def last_heartbeat(self) -> Optional[datetime]:
        """Return the UTC datetime of the last heartbeat, or None if no heartbeat recorded

        Returns:
            datetime: UTC datetime of the last heartbeat, or None if scheduler not found in registry
        """
        score = self.connection.zscore(cron_scheduler_registry.get_registry_key(), self.name)

        if score is None:
            return None

        # Convert Unix timestamp to UTC datetime
        return datetime.fromtimestamp(score, tz=timezone.utc)


# Global registry to store job data before Cron instance is created
_job_data_registry: List[Dict] = []


def register(
    func: Callable,
    queue_name: str,
    args: Optional[Tuple] = None,
    kwargs: Optional[Dict] = None,
    interval: Optional[int] = None,
    cron: Optional[str] = None,
    job_timeout: Optional[int] = None,
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
        'cron': cron,
        'job_timeout': job_timeout,
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
