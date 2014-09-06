# -*- coding: utf-8 -*-
"""
RQ command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys

import click
import redis
from rq import get_failed_queue, Queue
from rq.exceptions import InvalidJobOperationError

from .rqinfo import info


@click.group()
@click.option('--url', '-u', envvar='URL', help='URL describing Redis connection details.')
@click.pass_context
def main(ctx, url):
    """RQ CLI"""
    if url is None:
        url = "redis://localhost:6379/0"
    redis_conn = redis.from_url(url)

    ctx.obj = {}
    ctx.obj['connection'] = redis_conn


@main.command()
@click.option('--all', '-a', is_flag=True, help='Empty all queues')
@click.argument('queues', nargs=-1)
@click.pass_context
def empty(ctx, all, queues):
    """Empty given queues."""
    conn = ctx.obj['connection']
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
@click.option('--all', '-a', is_flag=True, help='Requeue all failed jobs')
@click.argument('job_ids', nargs=-1)
@click.pass_context
def requeue(ctx, all, job_ids):
    """Requeue failed jobs."""
    conn = ctx.obj['connection']
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


main.add_command(info)
