#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os
import time
import argparse
from redis.exceptions import ConnectionError
from rq import Queue, Worker
from rq.utils import gettermsize, make_colorizer
from rq.scripts import add_standard_arguments
from rq.scripts import setup_redis
from rq.scripts import read_config_file
from rq.scripts import setup_default_arguments

red = make_colorizer('darkred')
green = make_colorizer('darkgreen')
yellow = make_colorizer('darkyellow')


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


def show_queues(args):
    if len(args.queues):
        qs = list(map(Queue, args.queues))
    else:
        qs = Queue.all()

    num_jobs = 0
    termwidth, _ = gettermsize()
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
        if not args.raw:
            chart = green('|' + '█' * int(ratio * count))
            line = '%-12s %s %d' % (q.name, chart, count)
        else:
            line = 'queue %s %d' % (q.name, count)
        print(line)

        num_jobs += count

    # Print summary when not in raw mode
    if not args.raw:
        print('%d queues, %d jobs total' % (len(qs), num_jobs))


def show_workers(args):
    if len(args.queues):
        qs = list(map(Queue, args.queues))

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

    if not args.by_queue:
        for w in ws:
            worker_queues = filter_queues(w.queue_names())
            if not args.raw:
                print('%s %s: %s' % (w.name, state_symbol(w.state), ', '.join(worker_queues)))
            else:
                print('worker %s %s %s' % (w.name, w.state, ','.join(worker_queues)))
    else:
        # Create reverse lookup table
        queues = dict([(q, []) for q in qs])
        for w in ws:
            for q in w.queues:
                if not q in queues:
                    continue
                queues[q].append(w)

        max_qname = max(map(lambda q: len(q.name), queues.keys())) if queues else 0
        for q in queues:
            if queues[q]:
                queues_str = ", ".join(sorted(map(lambda w: '%s (%s)' % (w.name, state_symbol(w.state)), queues[q])))
            else:
                queues_str = '–'
            print('%s %s' % (pad(q.name + ':', max_qname + 1), queues_str))

    if not args.raw:
        print('%d workers, %d queues' % (len(ws), len(qs)))


def show_both(args):
    show_queues(args)
    if not args.raw:
        print('')
    show_workers(args)
    if not args.raw:
        print('')
        import datetime
        print('Updated: %s' % datetime.datetime.now())


def parse_args():
    parser = argparse.ArgumentParser(description='RQ command-line monitor.')
    add_standard_arguments(parser)
    parser.add_argument('--path', '-P', default='.', help='Specify the import path.')
    parser.add_argument('--interval', '-i', metavar='N', type=float, default=2.5, help='Updates stats every N seconds (default: don\'t poll)')
    parser.add_argument('--raw', '-r', action='store_true', default=False, help='Print only the raw numbers, no bar charts')
    parser.add_argument('--only-queues', '-Q', dest='only_queues', default=False, action='store_true', help='Show only queue info')
    parser.add_argument('--only-workers', '-W', dest='only_workers', default=False, action='store_true', help='Show only worker info')
    parser.add_argument('--by-queue', '-R', dest='by_queue', default=False, action='store_true', help='Shows workers by queue')
    parser.add_argument('queues', nargs='*', help='The queues to poll')
    return parser.parse_args()


def interval(val, func, args):
    while True:
        if val and sys.stdout.isatty():
            os.system('clear')
        func(args)
        if val and sys.stdout.isatty():
            time.sleep(val)
        else:
            break


def main():
    args = parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    settings = {}
    if args.config:
        settings = read_config_file(args.config)

    setup_default_arguments(args, settings)

    setup_redis(args)
    try:
        if args.only_queues:
            func = show_queues
        elif args.only_workers:
            func = show_workers
        else:
            func = show_both

        interval(args.interval, func, args)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        sys.exit(0)
