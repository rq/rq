# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime, timezone

from click.testing import CliRunner
from redis import Redis

from rq import Queue
from rq.cli import main
from rq.cli.helpers import read_config_file, CliConfig
from rq.job import Job
from rq.registry import FailedJobRegistry, ScheduledJobRegistry
from rq.serializers import JSONSerializer
from rq.worker import Worker, WorkerStatus

import pytest

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class TestRQCli(RQTestCase):

    @pytest.fixture(autouse=True)
    def set_tmpdir(self, tmpdir):
        self.tmpdir = tmpdir

    def assert_normal_execution(self, result):
        if result.exit_code == 0:
            return True
        else:
            print("Non normal execution")
            print("Exit Code: {}".format(result.exit_code))
            print("Output: {}".format(result.output))
            print("Exception: {}".format(result.exception))
            self.assertEqual(result.exit_code, 0)

    """Test rq_cli script"""
    def setUp(self):
        super(TestRQCli, self).setUp()
        db_num = self.testconn.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num
        self.connection = Redis.from_url(self.redis_url)

        job = Job.create(func=div_by_zero, args=(1, 2, 3))
        job.origin = 'fake'
        job.save()

    def test_config_file(self):
        settings = read_config_file('tests.config_files.dummy')
        self.assertIn('REDIS_HOST', settings)
        self.assertEqual(settings['REDIS_HOST'], 'testhost.example.com')

    def test_config_file_option(self):
        """"""
        cli_config = CliConfig(config='tests.config_files.dummy')
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )
        runner = CliRunner()
        result = runner.invoke(main, ['info', '--config', cli_config.config])
        self.assertEqual(result.exit_code, 1)

    def test_config_file_default_options(self):
        """"""
        cli_config = CliConfig(config='tests.config_files.dummy')

        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['port'],
            6379
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['db'],
            0
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['password'],
            None
        )

    def test_config_file_default_options_override(self):
        """"""
        cli_config = CliConfig(config='tests.config_files.dummy_override')

        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['port'],
            6378
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['db'],
            2
        )
        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['password'],
            '123'
        )

    def test_empty_nothing(self):
        """rq empty -u <url>"""
        runner = CliRunner()
        result = runner.invoke(main, ['empty', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertEqual(result.output.strip(), 'Nothing to do')

    def test_requeue(self):
        """rq requeue -u <url> --all"""
        connection = Redis.from_url(self.redis_url)
        queue = Queue('requeue', connection=connection)
        registry = queue.failed_job_registry

        runner = CliRunner()

        job = queue.enqueue(div_by_zero)
        job2 = queue.enqueue(div_by_zero)
        job3 = queue.enqueue(div_by_zero)

        worker = Worker([queue])
        worker.work(burst=True)

        self.assertIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(
            main,
            ['requeue', '-u', self.redis_url, '--queue', 'requeue', job.id]
        )
        self.assert_normal_execution(result)

        # Only the first specified job is requeued
        self.assertNotIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(
            main,
            ['requeue', '-u', self.redis_url, '--queue', 'requeue', '--all']
        )
        self.assert_normal_execution(result)
        # With --all flag, all failed jobs are requeued
        self.assertNotIn(job2, registry)
        self.assertNotIn(job3, registry)

    def test_info(self):
        """rq info -u <url>"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertIn('0 queues, 0 jobs total', result.output)

        queue = Queue(connection=self.connection)
        queue.enqueue(say_hello)

        result = runner.invoke(main, ['info', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertIn('1 queues, 1 jobs total', result.output)

    def test_info_only_queues(self):
        """rq info -u <url> --only-queues (-Q)"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-queues'])
        self.assert_normal_execution(result)
        self.assertIn('0 queues, 0 jobs total', result.output)

        queue = Queue(connection=self.connection)
        queue.enqueue(say_hello)

        result = runner.invoke(main, ['info', '-u', self.redis_url])
        self.assert_normal_execution(result)
        self.assertIn('1 queues, 1 jobs total', result.output)

    def test_info_only_workers(self):
        """rq info -u <url> --only-workers (-W)"""
        runner = CliRunner()
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-workers'])
        self.assert_normal_execution(result)
        self.assertIn('0 workers, 0 queue', result.output)

        result = runner.invoke(main, ['info', '--by-queue',
                                      '-u', self.redis_url, '--only-workers'])
        self.assert_normal_execution(result)
        self.assertIn('0 workers, 0 queue', result.output)

        worker = Worker(['default'], connection=self.connection)
        worker.register_birth()
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-workers'])
        self.assert_normal_execution(result)
        self.assertIn('1 workers, 0 queues', result.output)
        worker.register_death()

        queue = Queue(connection=self.connection)
        queue.enqueue(say_hello)
        result = runner.invoke(main, ['info', '-u', self.redis_url, '--only-workers'])
        self.assert_normal_execution(result)
        self.assertIn('0 workers, 1 queues', result.output)

        foo_queue = Queue(name='foo', connection=self.connection)
        foo_queue.enqueue(say_hello)

        bar_queue = Queue(name='bar', connection=self.connection)
        bar_queue.enqueue(say_hello)

        worker_1 = Worker([foo_queue, bar_queue], connection=self.connection)
        worker_1.register_birth()

        worker_2 = Worker([foo_queue, bar_queue], connection=self.connection)
        worker_2.register_birth()
        worker_2.set_state(WorkerStatus.BUSY)

        result = runner.invoke(main, ['info', 'foo', 'bar',
                                      '-u', self.redis_url, '--only-workers'])

        self.assert_normal_execution(result)
        self.assertIn('2 workers, 2 queues', result.output)

        result = runner.invoke(main, ['info', 'foo', 'bar', '--by-queue',
                                      '-u', self.redis_url, '--only-workers'])

        self.assert_normal_execution(result)
        # Ensure both queues' workers are shown
        self.assertIn('foo:', result.output)
        self.assertIn('bar:', result.output)
        self.assertIn('2 workers, 2 queues', result.output)

    def test_worker(self):
        """rq worker -u <url> -b"""
        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        self.assert_normal_execution(result)

    def test_worker_pid(self):
        """rq worker -u <url> /tmp/.."""
        pid = self.tmpdir.join('rq.pid')
        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--pid', str(pid)])
        self.assertTrue(len(pid.read()) > 0)
        self.assert_normal_execution(result)

    def test_worker_with_scheduler(self):
        """rq worker -u <url> --with-scheduler"""
        queue = Queue(connection=self.connection)
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        registry = ScheduledJobRegistry(queue=queue)

        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        self.assert_normal_execution(result)
        self.assertEqual(len(registry), 1)  # 1 job still scheduled

        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--with-scheduler'])
        self.assert_normal_execution(result)
        self.assertEqual(len(registry), 0)  # Job has been enqueued

    def test_worker_logging_options(self):
        """--quiet and --verbose logging options are supported"""
        runner = CliRunner()
        args = ['worker', '-u', self.redis_url, '-b']
        result = runner.invoke(main, args + ['--verbose'])
        self.assert_normal_execution(result)
        result = runner.invoke(main, args + ['--quiet'])
        self.assert_normal_execution(result)

        # --quiet and --verbose are mutually exclusive
        result = runner.invoke(main, args + ['--quiet', '--verbose'])
        self.assertNotEqual(result.exit_code, 0)

    def test_exception_handlers(self):
        """rq worker -u <url> -b --exception-handler <handler>"""
        connection = Redis.from_url(self.redis_url)
        q = Queue('default', connection=connection)
        runner = CliRunner()

        # If exception handler is not given, no custom exception handler is run
        job = q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in registry)

        # If disable-default-exception-handler is given, job is not moved to FailedJobRegistry
        job = q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b',
                             '--disable-default-exception-handler'])
        registry = FailedJobRegistry(queue=q)
        self.assertFalse(job in registry)

        # Both default and custom exception handler is run
        job = q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b',
                             '--exception-handler', 'tests.fixtures.add_meta'])
        registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        job.refresh()
        self.assertEqual(job.meta, {'foo': 1})

        # Only custom exception handler is run
        job = q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b',
                             '--exception-handler', 'tests.fixtures.add_meta',
                             '--disable-default-exception-handler'])
        registry = FailedJobRegistry(queue=q)
        self.assertFalse(job in registry)
        job.refresh()
        self.assertEqual(job.meta, {'foo': 1})

    def test_suspend_and_resume(self):
        """rq suspend -u <url>
           rq worker -u <url> -b
           rq resume -u <url>
        """
        runner = CliRunner()
        result = runner.invoke(main, ['suspend', '-u', self.redis_url])
        self.assert_normal_execution(result)

        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b'])
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(
            result.output.strip(),
            'RQ is currently suspended, to resume job execution run "rq resume"'
        )

        result = runner.invoke(main, ['resume', '-u', self.redis_url])
        self.assert_normal_execution(result)

    def test_suspend_with_ttl(self):
        """rq suspend -u <url> --duration=2
        """
        runner = CliRunner()
        result = runner.invoke(main, ['suspend', '-u', self.redis_url, '--duration', 1])
        self.assert_normal_execution(result)

    def test_suspend_with_invalid_ttl(self):
        """rq suspend -u <url> --duration=0
        """
        runner = CliRunner()
        result = runner.invoke(main, ['suspend', '-u', self.redis_url, '--duration', 0])

        self.assertEqual(result.exit_code, 1)
        self.assertIn("Duration must be an integer greater than 1", result.output)

    def test_serializer(self):
        """rq worker -u <url> --serializer <serializer>"""
        connection = Redis.from_url(self.redis_url)
        q = Queue('default', connection=connection, serializer=JSONSerializer)
        runner = CliRunner()
        job = q.enqueue(say_hello)
        runner.invoke(main, ['worker', '-u', self.redis_url,
                            '--serializer rq.serializer.JSONSerializer'])
        self.assertIn(job.id, q.job_ids)
