# -*- coding: utf-8 -*-
"""
RQ command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from functools import update_wrapper
import os
import sys

import click
from redis.exceptions import ConnectionError

from rq import Connection, Retry, __version__ as version
from rq.cli.helpers import (read_config_file, refresh,
                            setup_loghandlers_from_args,
                            show_both, show_queues, show_workers, CliConfig, job_func, parse_function_args,
                            parse_schedule)
from rq.contrib.legacy import cleanup_ghosts
from rq.defaults import (DEFAULT_CONNECTION_CLASS, DEFAULT_JOB_CLASS,
                         DEFAULT_QUEUE_CLASS, DEFAULT_WORKER_CLASS,
                         DEFAULT_RESULT_TTL, DEFAULT_WORKER_TTL,
                         DEFAULT_JOB_MONITORING_INTERVAL,
                         DEFAULT_LOGGING_FORMAT, DEFAULT_LOGGING_DATE_FORMAT,
                         DEFAULT_SERIALIZER_CLASS)
from rq.exceptions import InvalidJobOperationError
from rq.registry import FailedJobRegistry, clean_registries
from rq.utils import import_attribute, generate_function_string
from rq.serializers import DefaultSerializer
from rq.suspension import (suspend as connection_suspend,
                           resume as connection_resume, is_suspended)
from rq.worker_registration import clean_worker_registry
from rq.job import JobStatus



# Disable the warning that Click displays (as of Click version 5.0) when users
# use unicode_literals in Python 2.
# See http://click.pocoo.org/dev/python3/#unicode-literals for more details.
click.disable_unicode_literals_warning = True


shared_options = [
    click.option('--url', '-u',
                 envvar='RQ_REDIS_URL',
                 help='URL describing Redis connection details.'),
    click.option('--config', '-c',
                 envvar='RQ_CONFIG',
                 help='Module containing RQ settings.'),
    click.option('--worker-class', '-w',
                 envvar='RQ_WORKER_CLASS',
                 default=DEFAULT_WORKER_CLASS,
                 help='RQ Worker class to use'),
    click.option('--job-class', '-j',
                 envvar='RQ_JOB_CLASS',
                 default=DEFAULT_JOB_CLASS,
                 help='RQ Job class to use'),
    click.option('--queue-class',
                 envvar='RQ_QUEUE_CLASS',
                 default=DEFAULT_QUEUE_CLASS,
                 help='RQ Queue class to use'),
    click.option('--connection-class',
                 envvar='RQ_CONNECTION_CLASS',
                 default=DEFAULT_CONNECTION_CLASS,
                 help='Redis client class to use'),
    click.option('--path', '-P',
                 default=['.'],
                 help='Specify the import path.',
                 multiple=True),
    click.option('--serializer', '-S',
                 default=DEFAULT_SERIALIZER_CLASS,
                 help='Path to serializer, defaults to rq.serializers.DefaultSerializer')
]


def pass_cli_config(func):
    # add all the shared options to the command
    for option in shared_options:
        func = option(func)

    # pass the cli config object into the command
    def wrapper(*args, **kwargs):
        ctx = click.get_current_context()
        cli_config = CliConfig(**kwargs)
        return ctx.invoke(func, cli_config, *args[1:], **kwargs)

    return update_wrapper(wrapper, func)


@click.group()
@click.version_option(version)
def main():
    """RQ command line tool."""
    pass


@main.command()
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
@pass_cli_config
def empty(cli_config, all, queues, **options):
    """Empty given queues."""

    if all:
        queues = cli_config.queue_class.all(connection=cli_config.connection,
                                            job_class=cli_config.job_class)
    else:
        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]

    if not queues:
        click.echo('Nothing to do')
        sys.exit(0)

    for queue in queues:
        num_jobs = queue.empty()
        click.echo('{0} jobs removed from {1} queue'.format(num_jobs, queue.name))


@main.command()
@click.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@click.option('--queue', required=True, type=str)
@click.argument('job_ids', nargs=-1)
@pass_cli_config
def requeue(cli_config, queue, all, job_class, job_ids, **options):
    """Requeue failed jobs."""

    failed_job_registry = FailedJobRegistry(queue,
                                            connection=cli_config.connection)
    if all:
        job_ids = failed_job_registry.get_job_ids()

    if not job_ids:
        click.echo('Nothing to do')
        sys.exit(0)

    click.echo('Requeueing {0} jobs from failed queue'.format(len(job_ids)))
    fail_count = 0
    with click.progressbar(job_ids) as job_ids:
        for job_id in job_ids:
            try:
                failed_job_registry.requeue(job_id)
            except InvalidJobOperationError:
                fail_count += 1

    if fail_count > 0:
        click.secho('Unable to requeue {0} jobs from failed job registry'.format(fail_count), fg='red')


@main.command()
@click.option('--interval', '-i', type=float, help='Updates stats every N seconds (default: don\'t poll)')
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
@pass_cli_config
def info(cli_config, interval, raw, only_queues, only_workers, by_queue, queues,
         **options):
    """RQ command-line monitor."""

    if only_queues:
        func = show_queues
    elif only_workers:
        func = show_workers
    else:
        func = show_both

    try:
        with Connection(cli_config.connection):

            if queues:
                qs = list(map(cli_config.queue_class, queues))
            else:
                qs = cli_config.queue_class.all()

            for queue in qs:
                clean_registries(queue)
                clean_worker_registry(queue)

            refresh(interval, func, qs, raw, by_queue,
                    cli_config.queue_class, cli_config.worker_class)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


@main.command()
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--logging_level', type=str, default="INFO", help='Set logging level')
@click.option('--log-format', type=str, default=DEFAULT_LOGGING_FORMAT, help='Set the format of the logs')
@click.option('--date-format', type=str, default=DEFAULT_LOGGING_DATE_FORMAT, help='Set the date format of the logs')
@click.option('--name', '-n', help='Specify a different name')
@click.option('--results-ttl', type=int, default=DEFAULT_RESULT_TTL, help='Default results timeout to be used')
@click.option('--worker-ttl', type=int, default=DEFAULT_WORKER_TTL, help='Default worker timeout to be used')
@click.option('--job-monitoring-interval', type=int, default=DEFAULT_JOB_MONITORING_INTERVAL, help='Default job monitoring interval to be used')
@click.option('--disable-job-desc-logging', is_flag=True, help='Turn off description logging.')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.option('--sentry-ca-certs', envvar='RQ_SENTRY_CA_CERTS', help='Path to CRT file for Sentry DSN')
@click.option('--sentry-debug', envvar='RQ_SENTRY_DEBUG', help='Enable debug')
@click.option('--sentry-dsn', envvar='RQ_SENTRY_DSN', help='Report exceptions to this Sentry DSN')
@click.option('--exception-handler', help='Exception handler(s) to use', multiple=True)
@click.option('--pid', help='Write the process ID number to a file at the specified path')
@click.option('--disable-default-exception-handler', '-d', is_flag=True, help='Disable RQ\'s default exception handler')
@click.option('--max-jobs', type=int, default=None, help='Maximum number of jobs to execute')
@click.option('--with-scheduler', '-s', is_flag=True, help='Run worker with scheduler')
@click.option('--serializer', '-S', default=None, help='Run worker with custom serializer')
@click.argument('queues', nargs=-1)
@pass_cli_config
def worker(cli_config, burst, logging_level, name, results_ttl,
           worker_ttl, job_monitoring_interval, disable_job_desc_logging,
           verbose, quiet, sentry_ca_certs, sentry_debug, sentry_dsn,
           exception_handler, pid, disable_default_exception_handler, max_jobs,
           with_scheduler, queues, log_format, date_format, serializer, **options):
    """Starts an RQ worker."""
    settings = read_config_file(cli_config.config) if cli_config.config else {}
    # Worker specific default arguments
    queues = queues or settings.get('QUEUES', ['default'])
    sentry_ca_certs = sentry_ca_certs or settings.get('SENTRY_CA_CERTS')
    sentry_debug = sentry_debug or settings.get('SENTRY_DEBUG')
    sentry_dsn = sentry_dsn or settings.get('SENTRY_DSN')
    name = name or settings.get('NAME')

    if pid:
        with open(os.path.expanduser(pid), "w") as fp:
            fp.write(str(os.getpid()))

    setup_loghandlers_from_args(verbose, quiet, date_format, log_format)

    try:
        cleanup_ghosts(cli_config.connection)
        exception_handlers = []
        for h in exception_handler:
            exception_handlers.append(import_attribute(h))

        if is_suspended(cli_config.connection):
            click.secho('RQ is currently suspended, to resume job execution run "rq resume"', fg='red')
            sys.exit(1)

        queues = [cli_config.queue_class(queue,
                                         connection=cli_config.connection,
                                         job_class=cli_config.job_class)
                  for queue in queues]
        worker = cli_config.worker_class(
            queues, name=name, connection=cli_config.connection,
            default_worker_ttl=worker_ttl, default_result_ttl=results_ttl,
            job_monitoring_interval=job_monitoring_interval,
            job_class=cli_config.job_class, queue_class=cli_config.queue_class,
            exception_handlers=exception_handlers or None,
            disable_default_exception_handler=disable_default_exception_handler,
            log_job_description=not disable_job_desc_logging,
            serializer=serializer
        )

        # Should we configure Sentry?
        if sentry_dsn:
            sentry_opts = {
                "ca_certs": sentry_ca_certs,
                "debug": sentry_debug
            }
            from rq.contrib.sentry import register_sentry
            register_sentry(sentry_dsn, **sentry_opts)

        # if --verbose or --quiet, override --logging_level
        if verbose or quiet:
            logging_level = None

        worker.work(burst=burst, logging_level=logging_level,
                    date_format=date_format, log_format=log_format,
                    max_jobs=max_jobs, with_scheduler=with_scheduler)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


@main.command()
@click.option('--duration', help='Seconds you want the workers to be suspended.  Default is forever.', type=int)
@pass_cli_config
def suspend(cli_config, duration, **options):
    """Suspends all workers, to resume run `rq resume`"""

    if duration is not None and duration < 1:
        click.echo("Duration must be an integer greater than 1")
        sys.exit(1)

    connection_suspend(cli_config.connection, duration)

    if duration:
        msg = """Suspending workers for {0} seconds.  No new jobs will be started during that time, but then will
        automatically resume""".format(duration)
        click.echo(msg)
    else:
        click.echo("Suspending workers.  No new jobs will be started.  But current jobs will be completed")


@main.command()
@pass_cli_config
def resume(cli_config, **options):
    """Resumes processing of queues, that were suspended with `rq suspend`"""
    connection_resume(cli_config.connection)
    click.echo("Resuming workers.")


@main.command(context_settings={'ignore_unknown_options': True})
@click.option('--queue', '-q', help='The name of the queue.', default='default')
@click.option('--timeout',
              help='Specifies the maximum runtime of the job before it’s interrupted and marked as failed.')
@click.option('--result-ttl', help='Specifies how long successful jobs and their results are kept.')
@click.option('--ttl', help='Specifies the maximum queued time of the job before it’s discarded.')
@click.option('--failure-ttl', help='Specifies how long failed jobs are kept.')
@click.option('--description', help='Additional description of the job')
@click.option('--depends-on', help='Specifies another job id that must complete before this job will be queued.',
              multiple=True)
@click.option('--job-id', help='The id of this job')
@click.option('--at-front', is_flag=True, help='Will place the job at the front of the queue, instead of the back')
@click.option('--retry-max', help='Maximum amound of retries', default=0, type=int)
@click.option('--retry-interval', help='Interval between retries in seconds', multiple=True, type=int, default=[0])
@click.option('--schedule-in', help='Timedelta in what the function is enqueued (e.g. 10s, 5m, 2d).')
@click.option('--schedule-at', help='Schedule job to be enqueued at a certain time formatted in ISO 8601 without '
              'timezone (e.g. 2021-05-27T21:45:00).')
@click.option('--quiet', is_flag=True, help='Only logs errors.')
@click.argument('func')
@click.argument('arguments', nargs=-1)
@pass_cli_config
def enqueue(cli_config, queue, timeout, result_ttl, ttl, failure_ttl, description, depends_on, job_id, at_front,
            retry_max, retry_interval, schedule_in, schedule_at, quiet, func, arguments, **options):
    """Enqueues a job from the command line"""
    args, kwargs = parse_function_args(arguments)

    description = description or generate_function_string(func, args, kwargs)

    retry = None
    if retry_max > 0:
        retry = Retry(retry_max, retry_interval)

    schedule = parse_schedule(schedule_in, schedule_at)

    with Connection(cli_config.connection):
        queue = cli_config.queue_class(queue)

        if schedule is None:
            job = queue.enqueue_call(job_func, (func, args, kwargs), {}, timeout, result_ttl, ttl, failure_ttl,
                                     description, depends_on, job_id, at_front, None, retry)
        else:
            job = queue.create_job(job_func, (func, args, kwargs), {}, timeout, result_ttl, ttl, failure_ttl,
                                   description, depends_on, job_id, None, JobStatus.SCHEDULED, retry)
            queue.schedule_job(job, schedule)

    if not quiet:
        click.echo('Enqueued with job-id \'%s\'' % job.id)
