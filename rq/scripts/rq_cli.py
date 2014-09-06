# -*- coding: utf-8 -*-
"""
RQ command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

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
@click.option('--all', '-a', 'is_all', is_flag=True, help='Requeue all failed jobs')
@click.argument('job_ids', nargs=-1)
@click.pass_context
def requeue(ctx, is_all, job_ids):
    """[JOB_IDS] Job_ids in FailedQueue to requeue

    \b
    $ rq requeue -a
    Requeueing 10 jobs from FailedQueue
      [####################################]  100%
    Unable to requeue 0 jobs from FailedQueue

    \b
    $ rq requeue a28bd044-65f3-42dd-96ee-acfcea155ba7\
 ac5559e1-90a0-4ab5-8e68-0d854b47b969
    Requeueing 2 jobs from FailedQueue
      [####################################]  100%
    Unable to requeue 1 jobs from FailedQueue
    """
    conn = ctx.obj['connection']
    failed_queue = get_failed_queue(connection=conn)
    if not job_ids and is_all:
        job_ids = failed_queue.job_ids
    click.echo('Requeueing {0} jobs from FailedQueue'.format(len(job_ids)))
    requeue_failed_num = 0
    with click.progressbar(job_ids) as job_bar:
        for job_id in job_bar:
            try:
                failed_queue.requeue(job_id)
            except InvalidJobOperationError:
                requeue_failed_num += 1

    click.secho('Unable to requeue {0} jobs from FailedQueue'.format(
        requeue_failed_num), fg='red')


main.add_command(info)
