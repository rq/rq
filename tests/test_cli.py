# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime, timezone
from time import sleep

from click.testing import CliRunner
from redis import Redis

from rq import Queue
from rq.cli import main
from rq.cli.helpers import read_config_file, CliConfig, job_func
from rq.job import Job
from rq.registry import FailedJobRegistry, ScheduledJobRegistry
from rq.serializers import JSONSerializer
from rq.worker import Worker, WorkerStatus
from rq.scheduler import RQScheduler

import pytest

from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


value = 0  # Used in executable_python_file.py and test_cli_enqueue_job_func()


def set_value(new_value):
    global value
    value = new_value


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

    def test_cli_enqueue(self):
        """rq enqueue -u <url> tests.fixtures.say_hello"""
        queue = Queue(connection=self.connection)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello'])
        self.assert_normal_execution(result)

        prefix = 'Enqueued with job-id \''
        suffix = '\'\n'

        print(result.stdout)

        self.assertTrue(result.stdout.startswith(prefix))
        self.assertTrue(result.stdout.endswith(suffix))

        job_id = result.stdout[len(prefix):-len(suffix)]
        queue_key = 'rq:queue:default'
        self.assertEqual(self.connection.llen(queue_key), 1)
        self.assertEqual(self.connection.lrange(queue_key, 0, -1)[0].decode('ascii'), job_id)

        worker = Worker(queue)
        worker.work(True)
        self.assertEqual(Job(job_id).result, 'Hi there, Stranger!')

    def test_cli_enqueue_args(self):
        """rq enqueue -u <url> tests.fixtures.echo hello -i 2 -f 3 -b true --bool N -k keyword value"""
        queue = Queue(connection=self.connection)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', 'hello', '-i', '2', '-f',
                                      '3', '-b', 'true', '--bool', 'N', '-k', 'keyword', 'value'])
        self.assert_normal_execution(result)

        job_id = self.connection.lrange('rq:queue:default', 0, -1)[0].decode('ascii')

        worker = Worker(queue)
        worker.work(True)

        args, kwargs = Job(job_id).result

        self.assertEqual(args, ('hello', 2, 3.0, True, False))
        self.assertEqual(kwargs, {'keyword': 'value'})

    def test_cli_enqueue_schedule_in(self):
        """rq enqueue -u <url> tests.fixtures.say_hello --schedule-in 1s"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        worker = Worker(queue)
        scheduler = RQScheduler(queue, self.connection)

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 0)

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello',
                                      '--schedule-in', '1s'])
        self.assert_normal_execution(result)

        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 1)

        self.assertFalse(worker.work(True))

        sleep(2)

        scheduler.enqueue_scheduled_jobs()

        self.assertTrue(len(queue) == 1)
        self.assertTrue(len(registry) == 0)

        self.assertTrue(worker.work(True))

    def test_cli_enqueue_schedule_at(self):
        """
        rq enqueue -u <url> tests.fixtures.say_hello --schedule-at 2021-01-01T00:00:00

        rq enqueue -u <url> tests.fixtures.say_hello --schedule-at 2100-01-01T00:00:00
        """
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        worker = Worker(queue)
        scheduler = RQScheduler(queue, self.connection)

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 0)

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello',
                                      '--schedule-at', '2021-01-01T00:00:00'])
        self.assert_normal_execution(result)

        scheduler.acquire_locks()

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 1)

        scheduler.enqueue_scheduled_jobs()

        self.assertTrue(len(queue) == 1)
        self.assertTrue(len(registry) == 0)

        self.assertTrue(worker.work(True))

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 0)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello',
                                      '--schedule-at', '2100-01-01T00:00:00'])
        self.assert_normal_execution(result)

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 1)

        scheduler.enqueue_scheduled_jobs()

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 1)

        self.assertFalse(worker.work(True))

    def test_cli_enqueue_retry(self):
        """rq enqueue -u <url> tests.fixtures.say_hello --retry-max 3 --retry-interval 10 --retry-interval 20
        --retry-interval 40"""
        queue = Queue(connection=self.connection)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello', '--retry-max', '3',
                                      '--retry-interval', '10', '--retry-interval', '20', '--retry-interval', '40'])
        self.assert_normal_execution(result)

        job = Job.fetch(self.connection.lrange('rq:queue:default', 0, -1)[0].decode('ascii'),
                        connection=self.connection)

        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [10, 20, 40])

    def test_cli_enqueue_errors(self):
        """
        rq enqueue -u <url> tests.fixtures.echo -i

        rq enqueue -u <url> tests.fixtures.echo -k

        rq enqueue -u <url> tests.fixtures.echo -k keyword

        rq enqueue -u <url> tests.fixtures.echo --schedule-in --schedule-at

        rq enqueue -u <url> tests.fixtures.echo -b maybe
        """
        runner = CliRunner()

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '-i'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('No value for type int specified.', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '-k'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('No keyword specified.', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '-k', 'keyword'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('No value for keyword \'keyword\' specified.', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '--schedule-in', '1s',
                                      '--schedule-at', '2000-01-01T00:00:00'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('You can\'t specify both --schedule-in and --schedule-at', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '-b', 'maybe'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Boolean must be \'y\', \'yes\', \'t\', \'true\', \'n\', \'no\', \'f\' or \'false\' '
                      '(case insensitive). Found: \'maybe\'', result.output)

    def test_job_func(self):
        """executes the rq.cli.helpers.job_func function"""
        self.assertEqual(job_func('tests.fixtures.echo', (1, 2.3, True), {'key': 'value'}),
                         ((1, 2.3, True), {'key': 'value'}))

        self.assertEqual(value, 0)
        job_func('tests.executable_python_file', None, None)
        self.assertEqual(value, 1)
