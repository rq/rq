import redis
from rq import use_connection


def add_standard_arguments(parser):
    parser.add_argument('--config', '-c', default=None,
            help='Module containing RQ settings.')
    parser.add_argument('--host', '-H', default=None,
            help='The Redis hostname (default: localhost)')
    parser.add_argument('--port', '-p', default=None,
            help='The Redis portnumber (default: 6379)')
    parser.add_argument('--db', '-d', type=int, default=None,
            help='The Redis database (default: 0)')
    parser.add_argument('--password', '-a', default=None,
            help='The Redis password (default: None)')


def read_config_file(module):
    """Reads all UPPERCASE variables defined in the given module file."""
    settings = __import__(module, [], [], [], -1)
    return dict([(k, v)
            for k, v in settings.__dict__.items()
            if k.upper() == k])


def setup_default_arguments(args, settings):
    """ Sets up args from settings or defaults """
    if args.host is None:
        args.host = settings.get('REDIS_HOST', 'localhost')

    if args.port is None:
        args.port = int(settings.get('REDIS_PORT', 6379))
    else:
        args.port = int(args.port)

    if args.db is None:
        args.db = settings.get('REDIS_DB', 0)

    if args.password is None:
        args.password = settings.get('REDIS_PASSWORD', None)


def setup_redis(args):
    redis_conn = redis.Redis(host=args.host, port=args.port, db=args.db,
        password=args.password)
    use_connection(redis_conn)
