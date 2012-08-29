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


def setup_redis(args):
    redis_conn = redis.Redis(host=args.host, port=args.port, db=args.db)
    use_connection(redis_conn)
