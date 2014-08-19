#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
import time
import click
from functools import partial

from redis.exceptions import ConnectionError
from rq import Queue, Worker


red = partial(click.style, fg='red') 
green = partial(click.style, fg='green')
yellow = partial(click.style, fg='yellow')


def pad(s, pad_to_length):
    """Pads the given string to the given length."""
    return ('%-' + '%ds' % pad_to_length) % (s,)


def get_scale(x):
    """Finds the lowest scale where x <= scale."""
    scales = [20, 50, 100, 200, 400, 600, 800, 1000]
    for scale in scales:
        if x <= scale:
            return scale
    return x


def state_symbol(state):
    symbols = {
        'busy': red('busy'),
        'idle': green('idle'),
    }
    try:
        return symbols[state]
    except KeyError:
        return state


def show_queues(queues, raw, by_queue):
    if len(queues):
        qs = list(map(Queue, queues))
    else:
        qs = Queue.all()

    num_jobs = 0
    termwidth, _ = click.get_terminal_size()
    chartwidth = min(20, termwidth - 20)

    max_count = 0
    counts = dict()
    for q in qs:
        count = q.count
        counts[q] = count
        max_count = max(max_count, count)
    scale = get_scale(max_count)
    ratio = chartwidth * 1.0 / scale

    for q in qs:
        count = counts[q]
        if not raw:
            chart = green('|' + '█' * int(ratio * count))
            line = '%-12s %s %d' % (q.name, chart, count)
        else:
            line = 'queue %s %d' % (q.name, count)
        click.echo(line)

        num_jobs += count

    # print summary when not in raw mode
    if not raw:
        click.echo('%d queues, %d jobs total' % (len(qs), num_jobs))


def show_workers(queues, raw, by_queue):
    if len(queues):
        qs = list(map(Queue, queues))

        def any_matching_queue(worker):
            def queue_matches(q):
                return q in qs
            return any(map(queue_matches, worker.queues))

        # Filter out workers that don't match the queue filter
        ws = [w for w in Worker.all() if any_matching_queue(w)]

        def filter_queues(queue_names):
            return [qname for qname in queue_names if Queue(qname) in qs]

    else:
        qs = Queue.all()
        ws = Worker.all()
        filter_queues = lambda x: x

    if not by_queue:
        for w in ws:
            worker_queues = filter_queues(w.queue_names())
            if not raw:
                click.echo('%s %s: %s' % (w.name, state_symbol(w.get_state()), ', '.join(worker_queues)))
            else:
                click.echo('worker %s %s %s' % (w.name, w.get_state(), ','.join(worker_queues)))
    else:
        # Create reverse lookup table
        queues = dict([(q, []) for q in qs])
        for w in ws:
            for q in w.queues:
                if q not in queues:
                    continue
                queues[q].append(w)

        max_qname = max(map(lambda q: len(q.name), queues.keys())) if queues else 0
        for q in queues:
            if queues[q]:
                queues_str = ", ".join(sorted(map(lambda w: '%s (%s)' % (w.name, state_symbol(w.get_state())), queues[q])))  # noqa
            else:
                queues_str = '–'
            click.echo('%s %s' % (pad(q.name + ':', max_qname + 1), queues_str))

    if not raw:
        click.echo('%d workers, %d queues' % (len(ws), len(qs)))


def show_both(queues, raw, by_queue):
    show_queues(queues, raw, by_queue)
    if not raw:
        click.echo('')
    show_workers(queues, raw, by_queue)
    if not raw:
        click.echo('')
        import datetime
        click.echo('Updated: %s' % datetime.datetime.now())


def refresh(val, func, *args):
    while True:
        if val:
            click.clear()
        func(*args)
        if val:
            time.sleep(val)
        else:
            break


@click.command()
@click.option('--path', '-P', default='.', help='Specify the import path.')
@click.option('--interval', '-i', default=2.5, help='Updates stats every N seconds (default: don\'t poll)')  # noqa
@click.option('--raw', '-r', is_flag=True, help='Print only the raw numbers, no bar charts')  # noqa
@click.option('--only-queues', '-Q', is_flag=True, help='Show only queue info')  # noqa
@click.option('--only-workers', '-W', is_flag=True, help='Show only worker info')  # noqa
@click.option('--by-queue', '-R', is_flag=True, help='Shows workers by queue')  # noqa
@click.argument('queues', nargs=-1)
def info(path, interval, raw, only_queues, only_workers, by_queue, queues):
    """RQ command-line monitor."""

    if path:
        sys.path = path.split(':') + sys.path

    try:
        if only_queues:
            func = show_queues
        elif only_workers:
            func = show_workers
        else:
            func = show_both

        refresh(interval, func, queues, raw, by_queue)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)
