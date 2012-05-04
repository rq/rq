#!/usr/bin/env python
import sys
import argparse
import logbook
from logbook import handlers
from rq import Queue, Worker
from redis.exceptions import ConnectionError
from rq.scripts import add_standard_arguments
from rq.scripts import setup_redis


def format_colors(record, handler):
    from rq.utils import make_colorizer
    if record.level == logbook.WARNING:
        colorize = make_colorizer('darkyellow')
    elif record.level >= logbook.ERROR:
        colorize = make_colorizer('darkred')
    else:
        colorize = lambda x: x
    return '%s: %s' % (record.time.strftime('%H:%M:%S'), colorize(record.msg))


def setup_loghandlers(args):
    if args.verbose:
        loglevel = logbook.DEBUG
        formatter = None
    else:
        loglevel = logbook.INFO
        formatter = format_colors

    handlers.NullHandler(bubble=False).push_application()
    handler = handlers.StreamHandler(sys.stdout, level=loglevel, bubble=False)
    if formatter:
        handler.formatter = formatter
    handler.push_application()
    handler = handlers.StderrHandler(level=logbook.WARNING, bubble=False)
    if formatter:
        handler.formatter = formatter
    handler.push_application()


def parse_args():
    parser = argparse.ArgumentParser(description='Starts an RQ worker.')
    add_standard_arguments(parser)

    parser.add_argument('--burst', '-b', action='store_true', default=False, help='Run in burst mode (quit after all work is done)')
    parser.add_argument('--name', '-n', default=None, help='Specify a different name')
    parser.add_argument('--path', '-P', default='.', help='Specify the import path.')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Show more output')
    parser.add_argument('queues', nargs='*', default=['default'], help='The queues to listen on (default: \'default\')')

    return parser.parse_args()


def main():
    args = parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    setup_loghandlers(args)
    setup_redis(args)
    try:
        queues = map(Queue, args.queues)
        w = Worker(queues, name=args.name)
        w.work(burst=args.burst)
    except ConnectionError as e:
        print(e)
