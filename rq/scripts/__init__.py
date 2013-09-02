import importlib
import redis
from warnings import warn
from rq import use_connection


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
    if args.url is None:
        args.url = settings.get('REDIS_URL')

    if (args.host or args.port or args.socket or args.db or args.password):
        warn('Host, port, db, password options for Redis will not be '
             'supported in future versions of RQ. '
             'Please use `REDIS_URL` or `--url` instead.', DeprecationWarning)

    if args.host is None:
        args.host = settings.get('REDIS_HOST', 'localhost')

    if args.port is None:
        args.port = int(settings.get('REDIS_PORT', 6379))
    else:
        args.port = int(args.port)

    socket = settings.get('REDIS_SOCKET', False)
    if args.socket is None and socket:
        args.socket = socket

    if args.db is None:
        args.db = settings.get('REDIS_DB', 0)

    if args.password is None:
        args.password = settings.get('REDIS_PASSWORD', None)


def setup_redis(args):
    if args.url is not None:
        redis_conn = redis.StrictRedis.from_url(args.url)
    else:
        redis_conn = redis.StrictRedis(host=args.host, port=args.port, db=args.db,
                                       password=args.password, unix_socket_path=args.socket)
    use_connection(redis_conn)
