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

def conf_pair(**faked_settings):
    return (faked_settings,
            uFaking('host', 'port', 'socket', 'db', 'queues', 'password'))

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
