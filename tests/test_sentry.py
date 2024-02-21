from unittest import mock

from click.testing import CliRunner

from rq import Queue
from rq.cli import main
from rq.cli.helpers import read_config_file
from rq.contrib.sentry import register_sentry
from rq.worker import SimpleWorker
from tests import RQTestCase
from tests.fixtures import div_by_zero


class FakeSentry:
    servers = []

    def captureException(self, *args, **kwds):  # noqa
        pass  # we cannot check this, because worker forks


class TestSentry(RQTestCase):
    def setUp(self):
        super().setUp()
        db_num = self.connection.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num

    def test_reading_dsn_from_file(self):
        settings = read_config_file('tests.config_files.sentry')
        self.assertIn('SENTRY_DSN', settings)
        self.assertEqual(settings['SENTRY_DSN'], 'https://123@sentry.io/123')

    @mock.patch('rq.contrib.sentry.register_sentry')
    def test_cli_flag(self, mocked):
        """rq worker -u <url> -b --exception-handler <handler>"""
        # connection = Redis.from_url(self.redis_url)
        runner = CliRunner()
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--sentry-dsn', 'https://1@sentry.io/1'])
        self.assertEqual(mocked.call_count, 1)

        runner.invoke(main, ['worker-pool', '-u', self.redis_url, '-b', '--sentry-dsn', 'https://1@sentry.io/1'])
        self.assertEqual(mocked.call_count, 2)

    def test_failure_capture(self):
        """Test failure is captured by Sentry SDK"""
        from sentry_sdk import Hub

        hub = Hub.current
        self.assertIsNone(hub.last_event_id())
        queue = Queue(connection=self.connection)
        queue.enqueue(div_by_zero)
        worker = SimpleWorker(queues=[queue], connection=self.connection)
        register_sentry('https://123@sentry.io/123')
        worker.work(burst=True)
        self.assertIsNotNone(hub.last_event_id())
