# -*- coding: utf-8 -*-
"""
RQ command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import sys

import click
from redis import StrictRedis
from redis.exceptions import ConnectionError

from rq import Connection, get_failed_queue, Queue
from rq.contrib.legacy import cleanup_ghosts
from rq.exceptions import InvalidJobOperationError
from rq.utils import import_attribute

from .helpers import (read_config_file, refresh, setup_loghandlers_from_args,
                      show_both, show_queues, show_workers)


url_option = click.option('--url', '-u', envvar='RQ_REDIS_URL',
                          help='URL describing Redis connection details.')


def connect(url):
    return StrictRedis.from_url(url or 'redis://localhost:6379/0')


@click.group()
def main():
    """RQ command line tool."""
    pass


@main.command()
@url_option
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
def empty(url, all, queues):
    """Empty given queues."""
    conn = connect(url)

    if all:
        queues = Queue.all(connection=conn)
    else:
        queues = [Queue(queue, connection=conn) for queue in queues]

    if not queues:
        click.echo('Nothing to do')

    for queue in queues:
        num_jobs = queue.empty()
        click.echo('{0} jobs removed from {1} queue'.format(num_jobs, queue.name))


@main.command()
@url_option
@click.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@click.argument('job_ids', nargs=-1)
def requeue(url, all, job_ids):
    """Requeue failed jobs."""
    conn = connect(url)
    failed_queue = get_failed_queue(connection=conn)

    if all:
        job_ids = failed_queue.job_ids

    if not job_ids:
        click.echo('Nothing to do')
        sys.exit(0)

    click.echo('Requeueing {0} jobs from failed queue'.format(len(job_ids)))
    fail_count = 0
    with click.progressbar(job_ids) as job_ids:
        for job_id in job_ids:
            try:
                failed_queue.requeue(job_id)
            except InvalidJobOperationError:
                fail_count += 1

    if fail_count > 0:
        click.secho('Unable to requeue {0} jobs from failed queue'.format(fail_count), fg='red')


@main.command()
@url_option
@click.option('--path', '-P', default='.', help='Specify the import path.')
@click.option('--interval', '-i', type=float, help='Updates stats every N seconds (default: don\'t poll)')
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')
@click.argument('queues', nargs=-1)
def info(url, path, interval, raw, only_queues, only_workers, by_queue, queues):
    """RQ command-line monitor."""

    if path:
        sys.path = path.split(':') + sys.path

    if only_queues:
        func = show_queues
    elif only_workers:
        func = show_workers
    else:
        func = show_both

    try:
        with Connection(connect(url)):
            refresh(interval, func, queues, raw, by_queue)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


@main.command()
@url_option
@click.option('--config', '-c', help='Module containing RQ settings.')
@click.option('--burst', '-b', is_flag=True, help='Run in burst mode (quit after all work is done)')
@click.option('--name', '-n', help='Specify a different name')
@click.option('--worker-class', '-w', default='rq.Worker', help='RQ Worker class to use')
@click.option('--job-class', '-j', default='rq.job.Job', help='RQ Job class to use')
@click.option('--queue-class', default='rq.Queue', help='RQ Queue class to use')
@click.option('--path', '-P', default='.', help='Specify the import path.')
@click.option('--results-ttl', help='Default results timeout to be used')
@click.option('--worker-ttl', type=int, help='Default worker timeout to be used')
@click.option('--verbose', '-v', is_flag=True, help='Show more output')
@click.option('--quiet', '-q', is_flag=True, help='Show less output')
@click.option('--sentry-dsn', envvar='SENTRY_DSN', help='Report exceptions to this Sentry DSN')
@click.option('--pid', help='Write the process ID number to a file at the specified path')
@click.argument('queues', nargs=-1)
def worker(url, config, burst, name, worker_class, job_class, queue_class, path, results_ttl, worker_ttl,
           verbose, quiet, sentry_dsn, pid, queues):
    """Starts an RQ worker."""

    if path:
        sys.path = path.split(':') + sys.path

    settings = read_config_file(config) if config else {}
    # Worker specific default arguments
    url = url or settings.get('REDIS_URL')
    queues = queues or settings.get('QUEUES', ['default'])
    sentry_dsn = sentry_dsn or settings.get('SENTRY_DSN')

    if pid:
        with open(os.path.expanduser(pid), "w") as fp:
            fp.write(str(os.getpid()))

    setup_loghandlers_from_args(verbose, quiet)

    conn = connect(url)
    cleanup_ghosts(conn)
    worker_class = import_attribute(worker_class)
    queue_class = import_attribute(queue_class)

    try:
        queues = [queue_class(queue, connection=conn) for queue in queues]
        w = worker_class(queues,
                         name=name,
                         connection=conn,
                         default_worker_ttl=worker_ttl,
                         default_result_ttl=results_ttl,
                         job_class=job_class)

        # Should we configure Sentry?
        if sentry_dsn:
            from raven import Client
            from rq.contrib.sentry import register_sentry
            client = Client(sentry_dsn)
            register_sentry(client, w)

        w.work(burst=burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
