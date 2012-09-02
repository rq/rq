#!/usr/bin/env python
import sys
import argparse
import logging
import logging.config

from rq import Queue, Worker
from redis.exceptions import ConnectionError
from rq.scripts import add_standard_arguments
from rq.scripts import setup_redis

logger = logging.getLogger(__name__)


def setup_loghandlers(args):
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,

        "formatters": {
            "console": {
                "format": "%(asctime)s %(message)s",
                "datefmt": "%H:%M:%S",
            },
        },

        "handlers": {
            "console": {
                "level": "DEBUG",
                #"class": "logging.StreamHandler",
                "class": "rq.utils.ColorizingStreamHandler",
                "formatter": "console",
                "exclude": ["%(asctime)s"],
            },
        },

        "root": {
            "handlers": ["console"],
            "level": "DEBUG" if args.verbose else "INFO"
        }
    })


def parse_args():
    parser = argparse.ArgumentParser(description='Starts an RQ worker.')
    add_standard_arguments(parser)

    parser.add_argument('--burst', '-b', action='store_true', default=False, help='Run in burst mode (quit after all work is done)')
    parser.add_argument('--name', '-n', default=None, help='Specify a different name')
    parser.add_argument('--path', '-P', default='.', help='Specify the import path.')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Show more output')
    parser.add_argument('--sentry-dsn', action='store', default=None, metavar='URL', help='Report exceptions to this Sentry DSN')
    parser.add_argument('queues', nargs='*', default=['default'], help='The queues to listen on (default: \'default\')')

    return parser.parse_args()


def read_config_file(module):
    """Reads all UPPERCASE variables defined in the given module file."""
    settings = __import__(module, [], [], [], -1)
    return {k: v for k, v in settings.__dict__.items() if k.upper() == k}


def main():
    args = parse_args()

    if args.path:
        sys.path = args.path.split(':') + sys.path

    settings = {}
    if args.config:
        settings = read_config_file(args.config)

    # Default arguments
    if args.host is None:
        args.host = settings.get('REDIS_HOST', 'localhost')
    if args.port is None:
        args.port = int(settings.get('REDIS_PORT', 6379))
    if args.db is None:
        args.db = settings.get('REDIS_DB', 0)
    if args.sentry_dsn is None:
        args.sentry_dsn = settings.get('SENTRY_DSN', None)

    setup_loghandlers(args)
    setup_redis(args)

    try:
        queues = map(Queue, args.queues)
        w = Worker(queues, name=args.name)

        # Should we configure Sentry?
        if args.sentry_dsn:
            from raven import Client
            from rq.contrib.sentry import register_sentry
            client = Client(args.sentry_dsn)
            register_sentry(client, w)

        w.work(burst=args.burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
