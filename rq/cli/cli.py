# -*- coding: utf-8 -*-
"""
RQ command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys

import click
from redis import StrictRedis
from redis.exceptions import ConnectionError
from rq import Connection, get_failed_queue, Queue
from rq.exceptions import InvalidJobOperationError

from .helpers import refresh, show_both, show_queues, show_workers

url_option = click.option('--url', '-u', envvar='URL', default='redis://localhost:6379/0',
                          help='URL describing Redis connection details.')


def connect(url):
    return StrictRedis.from_url(url)


@click.group()
def main():
    """RQ command line tool."""
    pass


@main.command()
@url_option
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
@click.pass_context
def empty(ctx, url, all, queues):
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
@click.pass_context
def requeue(ctx, url, all, job_ids):
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
@click.option('--interval', '-i', default=None, help='Updates stats every N seconds (default: don\'t poll)')  # noqa
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')  # noqa
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')  # noqa
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')  # noqa
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')  # noqa
@click.argument('queues', nargs=-1)
@click.pass_context
def info(ctx, url, path, interval, raw, only_queues, only_workers, by_queue, queues):
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
