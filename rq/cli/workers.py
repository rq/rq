import logging
import logging.config
import os
import sys
import warnings
from typing import TYPE_CHECKING, cast

import click
from redis.exceptions import ConnectionError

from rq import Worker
from rq.cli.cli import main
from rq.cli.helpers import (
    import_attribute,
    pass_cli_config,
    read_config_file,
    setup_loghandlers_from_args,
)
from rq.defaults import (
    DEFAULT_JOB_MONITORING_INTERVAL,
    DEFAULT_LOGGING_DATE_FORMAT,
    DEFAULT_LOGGING_FORMAT,
    DEFAULT_MAINTENANCE_TASK_INTERVAL,
    DEFAULT_RESULT_TTL,
    DEFAULT_WORKER_TTL,
)
from rq.job import Job
from rq.serializers import DefaultSerializer
from rq.suspension import is_suspended
from rq.utils import import_job_class, import_worker_class
from rq.worker_pool import WorkerPool

if TYPE_CHECKING:
    from rq.serializers import Serializer


@main.command()
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--logging_level', type=str, default=None, help='Set logging level')
@click.option('--log-format', type=str, default=DEFAULT_LOGGING_FORMAT, help='Set the format of the logs')
@click.option('--date-format', type=str, default=DEFAULT_LOGGING_DATE_FORMAT, help='Set the date format of the logs')
@click.option('--name', '-n', help='Specify a different name')
@click.option('--results-ttl', type=int, default=DEFAULT_RESULT_TTL, help='Default results timeout to be used')
@click.option('--worker-ttl', type=int, default=DEFAULT_WORKER_TTL, help='Worker timeout to be used')
@click.option(
    '--maintenance-interval',
    type=int,
    default=DEFAULT_MAINTENANCE_TASK_INTERVAL,
    help='Maintenance task interval (in seconds) to be used',
)
@click.option(
    '--job-monitoring-interval',
    type=int,
    default=DEFAULT_JOB_MONITORING_INTERVAL,
    help='Default job monitoring interval to be used',
)
@click.option('--disable-job-desc-logging', is_flag=True, help='Turn off description logging.')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.option('--exception-handler', help='Exception handler(s) to use', multiple=True)
@click.option('--pid', help='Write the process ID number to a file at the specified path')
@click.option('--disable-default-exception-handler', '-d', is_flag=True, help="Disable RQ's default exception handler")
@click.option('--max-jobs', type=int, default=None, help='Maximum number of jobs to execute')
@click.option('--max-idle-time', type=int, default=None, help='Maximum seconds to stay alive without jobs to execute')
@click.option('--with-scheduler', '-s', is_flag=True, help='Run worker with scheduler')
@click.option(
    '--dequeue-strategy', '-ds', default='default', help='Sets a custom stratey to dequeue from multiple queues'
)
@click.argument('queues', nargs=-1)
@pass_cli_config
def worker(
    cli_config,
    burst,
    logging_level,
    name,
    results_ttl,
    worker_ttl,
    maintenance_interval,
    job_monitoring_interval,
    disable_job_desc_logging,
    verbose,
    quiet,
    exception_handler,
    pid,
    disable_default_exception_handler,
    max_jobs,
    max_idle_time,
    with_scheduler,
    queues,
    log_format,
    date_format,
    serializer,
    dequeue_strategy,
    **options,
):
    """Starts an RQ worker."""
    settings = read_config_file(cli_config.config) if cli_config.config else {}
    # Worker specific default arguments
    queues = queues or settings.get('QUEUES', ['default'])
    name = name or settings.get('NAME')
    dict_config = settings.get('DICT_CONFIG')

    if dict_config:
        logging.config.dictConfig(dict_config)

    if pid:
        with open(os.path.expanduser(pid), 'w') as fp:
            fp.write(str(os.getpid()))

    worker_name = cli_config.worker_class.__qualname__
    if worker_name in ['RoundRobinWorker', 'RandomWorker']:
        strategy_alternative = 'random' if worker_name == 'RandomWorker' else 'round_robin'
        msg = f'WARNING: {worker_name} is deprecated. Use `--dequeue-strategy {strategy_alternative}` instead.'
        warnings.warn(msg, DeprecationWarning)
        click.secho(msg, fg='yellow')

    if dequeue_strategy not in ('default', 'random', 'round_robin'):
        click.secho(
            'ERROR: Dequeue Strategy can only be one of `default`, `random` or `round_robin`.', err=True, fg='red'
        )
        sys.exit(1)

    setup_loghandlers_from_args(verbose, quiet, date_format, log_format)

    try:
        exception_handlers = []
        for h in exception_handler:
            exception_handlers.append(import_attribute(h))

        if is_suspended(cli_config.connection):
            click.secho('RQ is currently suspended, to resume job execution run "rq resume"', fg='red')
            sys.exit(1)

        queues = [
            cli_config.queue_class(
                queue, connection=cli_config.connection, job_class=cli_config.job_class, serializer=serializer
            )
            for queue in queues
        ]
        worker_instance = cli_config.worker_class(
            queues,
            name=name,
            connection=cli_config.connection,
            default_worker_ttl=worker_ttl,  # TODO remove this arg in 2.0
            worker_ttl=worker_ttl,
            default_result_ttl=results_ttl,
            maintenance_interval=maintenance_interval,
            job_monitoring_interval=job_monitoring_interval,
            job_class=cli_config.job_class,
            queue_class=cli_config.queue_class,
            exception_handlers=exception_handlers or None,
            disable_default_exception_handler=disable_default_exception_handler,
            log_job_description=not disable_job_desc_logging,
            serializer=serializer,
        )

        # if --verbose or --quiet, don't override the logging level set by setup_loghandlers_from_args
        if verbose or quiet:
            logging_level = None

        worker_instance.work(
            burst=burst,
            logging_level=logging_level,
            date_format=date_format,
            log_format=log_format,
            max_jobs=max_jobs,
            max_idle_time=max_idle_time,
            with_scheduler=with_scheduler,
            dequeue_strategy=dequeue_strategy,
        )
    except ConnectionError as e:
        logging.error(e)
        sys.exit(1)
    except Exception as e:
        # Catch other potential exceptions during setup
        logging.error(e, exc_info=True)
        sys.exit(1)


@main.command()
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--logging-level', '-l', type=str, default='INFO', help='Set logging level')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.option('--log-format', type=str, default=DEFAULT_LOGGING_FORMAT, help='Set the format of the logs')
@click.option('--date-format', type=str, default=DEFAULT_LOGGING_DATE_FORMAT, help='Set the date format of the logs')
@click.option('--job-class', type=str, default=None, help='Dotted path to a Job class')
@click.argument('queues', nargs=-1)
@click.option('--num-workers', '-n', type=int, default=1, help='Number of workers to start')
@pass_cli_config
def worker_pool(
    cli_config,
    burst: bool,
    logging_level,
    queues,
    serializer,
    verbose,
    quiet,
    log_format,
    date_format,
    worker_class,
    job_class,
    num_workers,
    **options,
):
    """Starts a RQ worker pool"""
    settings = read_config_file(cli_config.config) if cli_config.config else {}
    # Worker specific default arguments
    queue_names: list[str] = queues or settings.get('QUEUES', ['default'])

    setup_loghandlers_from_args(verbose, quiet, date_format, log_format)

    if serializer:
        serializer = cast('Serializer', import_attribute(serializer))
    else:
        serializer = DefaultSerializer

    if worker_class:
        worker_class = import_worker_class(worker_class)
    else:
        worker_class = Worker

    if job_class:
        job_class = import_job_class(job_class)
    else:
        job_class = Job

    # if --verbose or --quiet, use the appropriate logging level
    if verbose:
        logging_level = 'DEBUG'
    elif quiet:
        logging_level = 'WARNING'

    pool = WorkerPool(
        queue_names,
        connection=cli_config.connection,
        num_workers=num_workers,
        serializer=serializer,
        worker_class=worker_class,
        job_class=job_class,
    )
    pool.start(burst=burst, logging_level=logging_level)
