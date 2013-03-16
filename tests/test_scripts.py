from rq.compat import is_python_version
if is_python_version((2, 7), (3, 2)):
    from unittest import TestCase
else:
    from unittest2 import TestCase  # noqa
from rq.scripts import read_config_file, setup_redis, add_standard_arguments, setup_default_arguments

class uFaking(object):
    def __init__(self, *faking):
        for attr in faking:
            setattr(self, attr, None)

def fake_args():
    return uFaking('host', 'port', 'url', 'socket', 'db', 'queues', 'password')

def conf_pair(**faked_settings):
    return faked_settings, fake_args()


class TestScripts(TestCase):
    def test_config_file(self):
        settings = read_config_file("tests.dummy_settings")
        self.assertIn("REDIS_HOST", settings)
        self.assertEqual(settings['REDIS_HOST'], "testhost.example.com")

    def test_socket_from_module(self):
        settings, args = conf_pair(REDIS_SOCKET='dummy')
        setup_default_arguments(args, settings)
        self.assertEqual(args.socket, 'dummy')

    def test_socket_from_args(self):
        settings, args = conf_pair(REDIS_SOCKET='sock-read-from-config')
        args.socket = 'sock-from-arg'
        setup_default_arguments(args, settings)
        self.assertEqual(args.socket, 'sock-from-arg')

    def test_add_socket_argument(self):
        caught = []

        args = uFaking()
        args.add_argument = lambda *args, **kw: caught.append(args)

        add_standard_arguments(args)
        socket_parms = [e for e in caught if e[0] == '--socket']
        self.assertEqual([('--socket', '-s')], socket_parms)

    def test_redis_socket_whiteboxish(self):
        args = fake_args()
        args.socket = '/no/such/path'
        setup_redis(args)

        from rq import connections
        conn = connections.pop_connection()
        from redis.connection import UnixDomainSocketConnection
        self.assertEqual(conn.connection_pool.connection_kwargs['path'], '/no/such/path')
