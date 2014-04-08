import importlib
import os
from functools import partial
from warnings import warn

import redis

from rq import use_connection
from rq.utils import first


def add_standard_arguments(parser):
    parser.add_argument('--config', '-c', default=None,
                        help='Module containing RQ settings.')
    parser.add_argument('--url', '-u', default=None,
                        help='URL describing Redis connection details. '
                             'Overrides other connection arguments if supplied.')
    parser.add_argument('--host', '-H', default=None,
                        help='The Redis hostname (default: localhost)')
    parser.add_argument('--port', '-p', default=None,
                        help='The Redis portnumber (default: 6379)')
    parser.add_argument('--db', '-d', type=int, default=None,
                        help='The Redis database (default: 0)')
    parser.add_argument('--password', '-a', default=None,
                        help='The Redis password (default: None)')
    parser.add_argument('--socket', '-s', default=None,
                        help='The Redis Unix socket')


def read_config_file(module):
    """Reads all UPPERCASE variables defined in the given module file."""
    settings = importlib.import_module(module)
    return dict([(k, v)
                 for k, v in settings.__dict__.items()
                 if k.upper() == k])


def setup_default_arguments(args, settings):
    """ Sets up args from settings or defaults """
    args.url = first([args.url, settings.get('REDIS_URL'), os.environ.get('RQ_REDIS_URL')])

    if (args.host or args.port or args.socket or args.db or args.password):
        warn('Host, port, db, password options for Redis will not be '
             'supported in future versions of RQ. '
             'Please use `REDIS_URL` or `--url` instead.', DeprecationWarning)

    strict_first = partial(first, key=lambda obj: obj is not None)

    args.host = strict_first([args.host, settings.get('REDIS_HOST'), os.environ.get('RQ_REDIS_HOST'), 'localhost'])
    args.port = int(strict_first([args.port, settings.get('REDIS_PORT'), os.environ.get('RQ_REDIS_PORT'), 6379]))
    args.socket = strict_first([args.socket, settings.get('REDIS_SOCKET'), os.environ.get('RQ_REDIS_SOCKET'), None])
    args.db = strict_first([args.db, settings.get('REDIS_DB'), os.environ.get('RQ_REDIS_DB'), 0])
    args.password = strict_first([args.password, settings.get('REDIS_PASSWORD'), os.environ.get('RQ_REDIS_PASSWORD')])


def setup_redis(args):
    if args.url is not None:
        redis_conn = redis.StrictRedis.from_url(args.url)
    else:
        redis_conn = redis.StrictRedis(host=args.host, port=args.port, db=args.db,
                                       password=args.password, unix_socket_path=args.socket)
    use_connection(redis_conn)
