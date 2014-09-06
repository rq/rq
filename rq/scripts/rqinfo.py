#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse
import sys
import time
from functools import partial

import click
from redis.exceptions import ConnectionError
from rq import Connection, get_failed_queue, Queue, Worker
from rq.scripts import (add_standard_arguments, read_config_file,
                        setup_default_arguments, setup_redis)

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
    if queues:
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
    if queues:
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
@click.pass_context
def info(ctx, path, interval, raw, only_queues, only_workers, by_queue, queues):
    """RQ command-line monitor."""

    if path:
        sys.path = path.split(':') + sys.path

    conn = ctx.obj['connection']
    try:
        if only_queues:
            func = show_queues
        elif only_workers:
            func = show_workers
        else:
            func = show_both

        with Connection(conn):
            refresh(interval, func, queues, raw, by_queue)
    except ConnectionError as e:
        click.echo(e)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo()
        sys.exit(0)


# TODO: The following code is for backward compatibility, should be removed in future
def parse_args():
    parser = argparse.ArgumentParser(description='RQ command-line monitor.')
    add_standard_arguments(parser)
    parser.add_argument('--path', '-P', default='.', help='Specify the import path.')
    parser.add_argument('--interval', '-i', metavar='N', type=float, default=2.5, help='Updates stats every N seconds (default: don\'t poll)')  # noqa
    parser.add_argument('--raw', '-r', action='store_true', default=False, help='Print only the raw numbers, no bar charts')  # noqa
    parser.add_argument('--only-queues', '-Q', dest='only_queues', default=False, action='store_true', help='Show only queue info')  # noqa
    parser.add_argument('--only-workers', '-W', dest='only_workers', default=False, action='store_true', help='Show only worker info')  # noqa
    parser.add_argument('--by-queue', '-R', dest='by_queue', default=False, action='store_true', help='Shows workers by queue')  # noqa
    parser.add_argument('--empty-failed-queue', '-X', dest='empty_failed_queue', default=False, action='store_true', help='Empties the failed queue, then quits')  # noqa
    parser.add_argument('queues', nargs='*', help='The queues to poll')
    return parser.parse_args()


def main():
    # warn users this command is deprecated, use `rq info`
    import warnings
    warnings.simplefilter('always', DeprecationWarning)
    warnings.warn("This command will be removed in future, "
                  "use `rq info` instead", DeprecationWarning)

    args = parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    settings = {}
    if args.config:
        settings = read_config_file(args.config)

    setup_default_arguments(args, settings)

    setup_redis(args)

    try:
        if args.empty_failed_queue:
            num_jobs = get_failed_queue().empty()
            print('{} jobs removed from failed queue'.format(num_jobs))
        else:
            if args.only_queues:
                func = show_queues
            elif args.only_workers:
                func = show_workers
            else:
                func = show_both

            refresh(args.interval, func, args.queues, args.raw, args.by_queue)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        sys.exit(0)
