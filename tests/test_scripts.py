from rq.compat import is_python_version
if is_python_version((2, 7), (3, 2)):
    from unittest import TestCase
else:
    from unittest2 import TestCase  # noqa

import argparse

from redis import StrictRedis
from rq.connections import get_current_connection
from rq.scripts import (
    read_config_file,
    setup_redis,
    setup_default_arguments
)

from rq.scripts import rqworker


class TestScripts(TestCase):
    def test_config_file(self):
        settings = read_config_file("tests.dummy_settings")
        self.assertIn("REDIS_HOST", settings)
        self.assertEqual(settings['REDIS_HOST'], "testhost.example.com")


class TestRQWorkerScript(TestCase):

    def setup_redis_with_arguments(self, arguments):
        parser = rqworker.setup_parser()
        args = parser.parse_args(arguments)

        setup_default_arguments(args, {})
        setup_redis(args)

    def assert_current_connection(self, expected_host, expected_port,
                                  expected_db):
        connection = get_current_connection()
        connection_kwargs = connection.connection_pool.connection_kwargs

        self.assertEqual(connection_kwargs["host"], expected_host)
        self.assertEqual(connection_kwargs["port"], expected_port)
        self.assertEqual(connection_kwargs["db"], expected_db)

    def test_setup_connection_from_url_defaults_to_default_db(self):
        self.setup_redis_with_arguments([
            "--url", "redis://localhost:6379",
        ])

        self.assert_current_connection("localhost", 6379, 0)

    def test_setup_connection_from_url_uses_correct_non_default_db(self):
        self.setup_redis_with_arguments([
            "--url", "redis://localhost:6379/1",
        ])

        self.assert_current_connection("localhost", 6379, 1)

    def test_url_overrides_db_argument(self):
        self.setup_redis_with_arguments([
            "--url", "redis://localhost:6379/1",
            "--db", "2"
        ])

        self.assert_current_connection("localhost", 6379, 1)
