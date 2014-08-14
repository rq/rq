#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
rq command line tool
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import click
import redis
from rq import get_failed_queue, Queue, use_connection
from rq.exceptions import InvalidJobOperationError


@click.group()
@click.option('--url', '-u', envvar='URL',
        help='URL describing Redis connection details.')
def rq(url):
    if url is None:
        url = "redis://localhost:6379/0"
    redis_conn = redis.from_url(url)
    use_connection(redis_conn)


@rq.command()
@click.argument('queues', nargs=-1)
def empty(queues):
    """Empty queues, default: empty failed queue

    $ rq empty
    2 jobs removed from failed queue

    $ rq empty default high
    10 jobs removed from default queue
    2 jobs removed from high queue
    """
    queues = list(map(Queue, queues))
    if not queues:
        queues = (get_failed_queue(),)
    for queue in queues:
        num_jobs = queue.empty()
        print('{} jobs removed from {} queue'.format(num_jobs, queue.name))


@rq.command()
def requeue():
    """Requeue all failed jobs in failed queue"""
    failed_queue = get_failed_queue()
    job_ids = failed_queue.job_ids
    print('Requeuing {} failed jobs......'.format(len(job_ids)))
    requeue_failed_num = 0
    for job_id in job_ids:
        try:
            failed_queue.requeue(job_id)
        except InvalidJobOperationError:
            print('Requeue job({}) failed'.format(job_id))
            requeue_failed_num += 1

    print('Requeue over with {} jobs requeuing failed'.format(
        requeue_failed_num))
