"""
RQ command line tool
"""

import sys

import click
from redis.exceptions import ConnectionError

from rq import Retry
from rq import __version__ as version
from rq.cli.helpers import (
    parse_function_args,
    parse_schedule,
    pass_cli_config,
    refresh,
    show_both,
    show_queues,
    show_workers,
)

# from rq.cli.pool import pool
from rq.exceptions import InvalidJobOperationError
from rq.job import JobStatus
from rq.logutils import blue
from rq.registry import FailedJobRegistry, clean_registries
from rq.suspension import resume as connection_resume
from rq.suspension import suspend as connection_suspend
from rq.utils import get_call_string
from rq.worker_registration import clean_worker_registry


@click.group()
@click.version_option(version)
def main():
    """RQ command line tool."""
    pass


@main.command()
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
@pass_cli_config
def empty(cli_config, all, queues, serializer, **options):
    """Empty given queues."""

    if all:
        queues = cli_config.queue_class.all(
            connection=cli_config.connection,
            job_class=cli_config.job_class,
            death_penalty_class=cli_config.death_penalty_class,
            serializer=serializer,
        )
    else:
        queues = [
            cli_config.queue_class(
                queue, connection=cli_config.connection, job_class=cli_config.job_class, serializer=serializer
            )
            for queue in queues
        ]

    if not queues:
        click.echo('Nothing to do')
        sys.exit(0)

    for queue in queues:
        num_jobs = queue.empty()
        click.echo(f'{num_jobs} jobs removed from {queue.name} queue')


@main.command()
@click.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@click.option('--queue', required=True, type=str)
@click.argument('job_ids', nargs=-1)
@pass_cli_config
def requeue(cli_config, queue, all, job_class, serializer, job_ids, **options):
    """Requeue failed jobs."""

    failed_job_registry = FailedJobRegistry(
        queue, connection=cli_config.connection, job_class=cli_config.job_class, serializer=serializer
    )
    if all:
        job_ids = failed_job_registry.get_job_ids()

    if not job_ids:
        click.echo('Nothing to do')
        sys.exit(0)

    click.echo(f'Requeueing {len(job_ids)} jobs from failed queue')
    fail_count = 0
    with click.progressbar(job_ids) as job_ids:
        for job_id in job_ids:
            try:
                failed_job_registry.requeue(job_id)
            except InvalidJobOperationError:
                fail_count += 1

    if fail_count > 0:
        click.secho(f'Unable to requeue {fail_count} jobs from failed job registry', fg='red')


@main.command()
@click.option('--interval', '-i', type=float, help="Updates stats every N seconds (default: don't poll)")
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
@pass_cli_config
def info(cli_config, interval, raw, only_queues, only_workers, by_queue, queues, **options):
    """RQ command-line monitor."""

    if only_queues:
        func = show_queues
    elif only_workers:
        func = show_workers
    else:
        func = show_both

    try:
        if queues:
            qs = []
            for queue_name in queues:
                qs.append(cli_config.queue_class(queue_name, connection=cli_config.connection))
        else:
            qs = cli_config.queue_class.all(connection=cli_config.connection)

        for queue in qs:
            clean_registries(queue)
            clean_worker_registry(queue)

        refresh(
            interval, func, qs, raw, by_queue, cli_config.queue_class, cli_config.worker_class, cli_config.connection
        )
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


@main.command()
@click.option('--duration', help='Seconds you want the workers to be suspended.  Default is forever.', type=int)
@pass_cli_config
def suspend(cli_config, duration, **options):
    """Suspends all workers, to resume run `rq resume`"""

    if duration is not None and duration < 1:
        click.echo('Duration must be an integer greater than 1')
        sys.exit(1)

    connection_suspend(cli_config.connection, duration)

    if duration:
        msg = f"""Suspending workers for {duration} seconds. No new jobs will be started during that time, but then will
        automatically resume"""
        click.echo(msg)
    else:
        click.echo('Suspending workers.  No new jobs will be started.  But current jobs will be completed')


@main.command()
@pass_cli_config
def resume(cli_config, **options):
    """Resumes processing of queues, that were suspended with `rq suspend`"""
    connection_resume(cli_config.connection)
    click.echo('Resuming workers.')


@main.command()
@click.option('--queue', '-q', help='The name of the queue.', default='default')
@click.option(
    '--timeout', help='Specifies the maximum runtime of the job before it is interrupted and marked as failed.'
)
@click.option('--result-ttl', help='Specifies how long successful jobs and their results are kept.')
@click.option('--ttl', help='Specifies the maximum queued time of the job before it is discarded.')
@click.option('--failure-ttl', help='Specifies how long failed jobs are kept.')
@click.option('--description', help='Additional description of the job')
@click.option(
    '--depends-on', help='Specifies another job id that must complete before this job will be queued.', multiple=True
)
@click.option('--job-id', help='The id of this job')
@click.option('--at-front', is_flag=True, help='Will place the job at the front of the queue, instead of the end')
@click.option('--retry-max', help='Maximum amount of retries', default=0, type=int)
@click.option('--retry-interval', help='Interval between retries in seconds', multiple=True, type=int, default=[0])
@click.option('--schedule-in', help='Delay until the function is enqueued (e.g. 10s, 5m, 2d).')
@click.option(
    '--schedule-at',
    help='Schedule job to be enqueued at a certain time formatted in ISO 8601 without '
    'timezone (e.g. 2021-05-27T21:45:00).',
)
@click.option('--quiet', is_flag=True, help='Only logs errors.')
@click.argument('function')
@click.argument('arguments', nargs=-1)
@pass_cli_config
def enqueue(
    cli_config,
    queue,
    timeout,
    result_ttl,
    ttl,
    failure_ttl,
    description,
    depends_on,
    job_id,
    at_front,
    retry_max,
    retry_interval,
    schedule_in,
    schedule_at,
    quiet,
    serializer,
    function,
    arguments,
    **options,
):
    """Enqueues a job from the command line"""
    args, kwargs = parse_function_args(arguments)
    function_string = get_call_string(function, args, kwargs)
    description = description or function_string

    retry = None
    if retry_max > 0:
        retry = Retry(retry_max, retry_interval)

    schedule = parse_schedule(schedule_in, schedule_at)

    queue = cli_config.queue_class(queue, serializer=serializer, connection=cli_config.connection)

    if schedule is None:
        job = queue.enqueue_call(
            function,
            args,
            kwargs,
            timeout,
            result_ttl,
            ttl,
            failure_ttl,
            description,
            depends_on,
            job_id,
            at_front,
            None,
            retry,
        )
    else:
        job = queue.create_job(
            function,
            args,
            kwargs,
            timeout,
            result_ttl,
            ttl,
            failure_ttl,
            description,
            depends_on,
            job_id,
            None,
            JobStatus.SCHEDULED,
            retry,
        )
        queue.schedule_job(job, schedule)

    if not quiet:
        click.echo("Enqueued %s with job-id '%s'." % (blue(function_string), job.id))


if __name__ == '__main__':
    main()
