import json
import os
from datetime import datetime, timedelta, timezone
from time import sleep
from uuid import uuid4

import pytest
from click.testing import CliRunner
from redis import Redis

from rq import Queue
from rq.cli import main
from rq.cli.helpers import CliConfig, parse_function_arg, parse_schedule, read_config_file
from rq.job import Job, JobStatus
from rq.registry import FailedJobRegistry, ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.serializers import JSONSerializer
from rq.timeouts import UnixSignalDeathPenalty
from rq.worker import Worker, WorkerStatus
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class CLITestCase(RQTestCase):
    def setUp(self):
        super().setUp()
        db_num = self.connection.connection_pool.connection_kwargs['db']
        self.redis_url = 'redis://127.0.0.1:6379/%d' % db_num
        self.connection = Redis.from_url(self.redis_url)

    def assert_normal_execution(self, result):
        if result.exit_code == 0:
            return True
        else:
            print("Non normal execution")
            print("Exit Code: {}".format(result.exit_code))
            print("Output: {}".format(result.output))
            print("Exception: {}".format(result.exception))
            self.assertEqual(result.exit_code, 0)


class TestRQCli(CLITestCase):
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
        super().setUp()
        job = Job.create(func=div_by_zero, args=(1, 2, 3), connection=self.connection)
        job.origin = 'fake'
        job.save()

    def test_config_file(self):
        settings = read_config_file('tests.config_files.dummy')
        self.assertIn('REDIS_HOST', settings)
        self.assertEqual(settings['REDIS_HOST'], 'testhost.example.com')

    def test_config_file_logging(self):
        runner = CliRunner()
        result = runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '-c', 'tests.config_files.dummy_logging'])
        self.assert_normal_execution(result)

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
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['port'], 6379)
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['db'], 0)
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['password'], None)

    def test_config_file_default_options_override(self):
        """"""
        cli_config = CliConfig(config='tests.config_files.dummy_override')

        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['port'], 6378)
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['db'], 2)
        self.assertEqual(cli_config.connection.connection_pool.connection_kwargs['password'], '123')

    def test_config_env_vars(self):
        os.environ['REDIS_HOST'] = "testhost.example.com"

        cli_config = CliConfig()

        self.assertEqual(
            cli_config.connection.connection_pool.connection_kwargs['host'],
            'testhost.example.com',
        )

    def test_death_penalty_class(self):
        cli_config = CliConfig()

        self.assertEqual(UnixSignalDeathPenalty, cli_config.death_penalty_class)

        cli_config = CliConfig(death_penalty_class='rq.job.Job')
        self.assertEqual(Job, cli_config.death_penalty_class)

        with self.assertRaises(ValueError):
            CliConfig(death_penalty_class='rq.abcd')

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

        worker = Worker([queue], connection=connection)
        worker.work(burst=True)

        self.assertIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(main, ['requeue', '-u', self.redis_url, '--queue', 'requeue', job.id])
        self.assert_normal_execution(result)

        # Only the first specified job is requeued
        self.assertNotIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(main, ['requeue', '-u', self.redis_url, '--queue', 'requeue', '--all'])
        self.assert_normal_execution(result)
        # With --all flag, all failed jobs are requeued
        self.assertNotIn(job2, registry)
        self.assertNotIn(job3, registry)

    def test_requeue_with_serializer(self):
        """rq requeue -u <url> -S <serializer> --all"""
        connection = Redis.from_url(self.redis_url)
        queue = Queue('requeue', connection=connection, serializer=JSONSerializer)
        registry = queue.failed_job_registry

        runner = CliRunner()

        job = queue.enqueue(div_by_zero)
        job2 = queue.enqueue(div_by_zero)
        job3 = queue.enqueue(div_by_zero)

        worker = Worker([queue], serializer=JSONSerializer, connection=connection)
        worker.work(burst=True)

        self.assertIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(
            main, ['requeue', '-u', self.redis_url, '--queue', 'requeue', '-S', 'rq.serializers.JSONSerializer', job.id]
        )
        self.assert_normal_execution(result)

        # Only the first specified job is requeued
        self.assertNotIn(job, registry)
        self.assertIn(job2, registry)
        self.assertIn(job3, registry)

        result = runner.invoke(
            main,
            ['requeue', '-u', self.redis_url, '--queue', 'requeue', '-S', 'rq.serializers.JSONSerializer', '--all'],
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

        result = runner.invoke(main, ['info', '--by-queue', '-u', self.redis_url, '--only-workers'])
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

        result = runner.invoke(main, ['info', 'foo', 'bar', '-u', self.redis_url, '--only-workers'])

        self.assert_normal_execution(result)
        self.assertIn('2 workers, 2 queues', result.output)

        result = runner.invoke(main, ['info', 'foo', 'bar', '--by-queue', '-u', self.redis_url, '--only-workers'])

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

    def test_worker_dequeue_strategy(self):
        """--quiet and --verbose logging options are supported"""
        runner = CliRunner()
        args = ['worker', '-u', self.redis_url, '-b', '--dequeue-strategy', 'random']
        result = runner.invoke(main, args)
        self.assert_normal_execution(result)

        args = ['worker', '-u', self.redis_url, '-b', '--dequeue-strategy', 'round_robin']
        result = runner.invoke(main, args)
        self.assert_normal_execution(result)

        args = ['worker', '-u', self.redis_url, '-b', '--dequeue-strategy', 'wrong']
        result = runner.invoke(main, args)
        self.assertEqual(result.exit_code, 1)

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
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--disable-default-exception-handler'])
        registry = FailedJobRegistry(queue=q)
        self.assertFalse(job in registry)

        # Both default and custom exception handler is run
        job = q.enqueue(div_by_zero)
        runner.invoke(main, ['worker', '-u', self.redis_url, '-b', '--exception-handler', 'tests.fixtures.add_meta'])
        registry = FailedJobRegistry(queue=q)
        self.assertTrue(job in registry)
        job.refresh()
        self.assertEqual(job.meta, {'foo': 1})

        # Only custom exception handler is run
        job = q.enqueue(div_by_zero)
        runner.invoke(
            main,
            [
                'worker',
                '-u',
                self.redis_url,
                '-b',
                '--exception-handler',
                'tests.fixtures.add_meta',
                '--disable-default-exception-handler',
            ],
        )
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
        self.assertEqual(result.output.strip(), 'RQ is currently suspended, to resume job execution run "rq resume"')

        result = runner.invoke(main, ['resume', '-u', self.redis_url])
        self.assert_normal_execution(result)

    def test_suspend_with_ttl(self):
        """rq suspend -u <url> --duration=2"""
        runner = CliRunner()
        result = runner.invoke(main, ['suspend', '-u', self.redis_url, '--duration', 1])
        self.assert_normal_execution(result)

    def test_suspend_with_invalid_ttl(self):
        """rq suspend -u <url> --duration=0"""
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
        runner.invoke(main, ['worker', '-u', self.redis_url, '--serializer rq.serializer.JSONSerializer'])
        self.assertIn(job.id, q.job_ids)

    def test_cli_enqueue(self):
        """rq enqueue -u <url> tests.fixtures.say_hello"""
        queue = Queue(connection=self.connection)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello'])
        self.assert_normal_execution(result)

        prefix = 'Enqueued tests.fixtures.say_hello() with job-id \''
        suffix = '\'.\n'

        self.assertTrue(result.output.startswith(prefix))
        self.assertTrue(result.output.endswith(suffix))

        job_id = result.output[len(prefix) : -len(suffix)]
        queue_key = 'rq:queue:default'
        self.assertEqual(self.connection.llen(queue_key), 1)
        self.assertEqual(self.connection.lrange(queue_key, 0, -1)[0].decode('ascii'), job_id)

        worker = Worker(queue, connection=self.connection)
        worker.work(True)
        self.assertEqual(Job(job_id, connection=self.connection).result, 'Hi there, Stranger!')

    def test_cli_enqueue_with_serializer(self):
        """rq enqueue -u <url> -S rq.serializers.JSONSerializer tests.fixtures.say_hello"""
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '-S', 'rq.serializers.JSONSerializer', 'tests.fixtures.say_hello']
        )
        self.assert_normal_execution(result)

        prefix = 'Enqueued tests.fixtures.say_hello() with job-id \''
        suffix = '\'.\n'

        self.assertTrue(result.output.startswith(prefix))
        self.assertTrue(result.output.endswith(suffix))

        job_id = result.output[len(prefix) : -len(suffix)]
        queue_key = 'rq:queue:default'
        self.assertEqual(self.connection.llen(queue_key), 1)
        self.assertEqual(self.connection.lrange(queue_key, 0, -1)[0].decode('ascii'), job_id)

        worker = Worker(queue, serializer=JSONSerializer, connection=self.connection)
        worker.work(True)
        self.assertEqual(
            Job(job_id, serializer=JSONSerializer, connection=self.connection).result, 'Hi there, Stranger!'
        )

    def test_cli_enqueue_args(self):
        """rq enqueue -u <url> tests.fixtures.echo hello ':[1, {"key": "value"}]' json:=["abc"] nojson=def"""
        queue = Queue(connection=self.connection)
        self.assertTrue(queue.is_empty())

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                'enqueue',
                '-u',
                self.redis_url,
                'tests.fixtures.echo',
                'hello',
                ':[1, {"key": "value"}]',
                ':@tests/test.json',
                '%1, 2',
                'json:=[3.0, true]',
                'nojson=abc',
                'file=@tests/test.json',
            ],
        )
        self.assert_normal_execution(result)

        job_id = self.connection.lrange('rq:queue:default', 0, -1)[0].decode('ascii')

        worker = Worker(queue, connection=self.connection)
        worker.work(True)

        args, kwargs = Job(job_id, connection=self.connection).result

        self.assertEqual(args, ('hello', [1, {'key': 'value'}], {"test": True}, (1, 2)))
        self.assertEqual(kwargs, {'json': [3.0, True], 'nojson': 'abc', 'file': '{"test": true}\n'})

    def test_cli_enqueue_schedule_in(self):
        """rq enqueue -u <url> tests.fixtures.say_hello --schedule-in 1s"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        worker = Worker(queue, connection=self.connection)
        scheduler = RQScheduler(queue, self.connection)

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 0)

        runner = CliRunner()
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello', '--schedule-in', '10s']
        )
        self.assert_normal_execution(result)

        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 1)

        self.assertFalse(worker.work(True))

        sleep(11)

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
        worker = Worker(queue, connection=self.connection)
        scheduler = RQScheduler(queue, self.connection)

        self.assertTrue(len(queue) == 0)
        self.assertTrue(len(registry) == 0)

        runner = CliRunner()
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello', '--schedule-at', '2021-01-01T00:00:00']
        )
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

        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.say_hello', '--schedule-at', '2100-01-01T00:00:00']
        )
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
        result = runner.invoke(
            main,
            [
                'enqueue',
                '-u',
                self.redis_url,
                'tests.fixtures.say_hello',
                '--retry-max',
                '3',
                '--retry-interval',
                '10',
                '--retry-interval',
                '20',
                '--retry-interval',
                '40',
            ],
        )
        self.assert_normal_execution(result)

        job = Job.fetch(
            self.connection.lrange('rq:queue:default', 0, -1)[0].decode('ascii'), connection=self.connection
        )

        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [10, 20, 40])

    def test_cli_enqueue_errors(self):
        """
        rq enqueue -u <url> tests.fixtures.echo :invalid_json

        rq enqueue -u <url> tests.fixtures.echo %invalid_eval_statement

        rq enqueue -u <url> tests.fixtures.echo key=value key=value

        rq enqueue -u <url> tests.fixtures.echo --schedule-in 1s --schedule-at 2000-01-01T00:00:00

        rq enqueue -u <url> tests.fixtures.echo @not_existing_file
        """
        runner = CliRunner()

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', ':invalid_json'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Unable to parse 1. non keyword argument as JSON.', result.output)

        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '%invalid_eval_statement']
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Unable to eval 1. non keyword argument as Python object.', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', 'key=value', 'key=value'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('You can\'t specify multiple values for the same keyword.', result.output)

        result = runner.invoke(
            main,
            [
                'enqueue',
                '-u',
                self.redis_url,
                'tests.fixtures.echo',
                '--schedule-in',
                '1s',
                '--schedule-at',
                '2000-01-01T00:00:00',
            ],
        )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('You can\'t specify both --schedule-in and --schedule-at', result.output)

        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, 'tests.fixtures.echo', '@not_existing_file'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Not found', result.output)

    def test_parse_schedule(self):
        """executes the rq.cli.helpers.parse_schedule function"""
        self.assertEqual(parse_schedule(None, '2000-01-23T23:45:01'), datetime(2000, 1, 23, 23, 45, 1))

        start = datetime.now(timezone.utc) + timedelta(minutes=5)
        middle = parse_schedule('5m', None)
        end = datetime.now(timezone.utc) + timedelta(minutes=5)

        self.assertGreater(middle, start)
        self.assertLess(middle, end)

    def test_parse_function_arg(self):
        """executes the rq.cli.helpers.parse_function_arg function"""
        self.assertEqual(parse_function_arg('abc', 0), (None, 'abc'))
        self.assertEqual(parse_function_arg(':{"json": true}', 1), (None, {'json': True}))
        self.assertEqual(parse_function_arg('%1, 2', 2), (None, (1, 2)))
        self.assertEqual(parse_function_arg('key=value', 3), ('key', 'value'))
        self.assertEqual(parse_function_arg('jsonkey:=["json", "value"]', 4), ('jsonkey', ['json', 'value']))
        self.assertEqual(parse_function_arg('evalkey%=1.2', 5), ('evalkey', 1.2))
        self.assertEqual(parse_function_arg(':@tests/test.json', 6), (None, {'test': True}))
        self.assertEqual(parse_function_arg('@tests/test.json', 7), (None, '{"test": true}\n'))

    def test_cli_enqueue_doc_test(self):
        """tests the examples of the documentation"""
        runner = CliRunner()

        id = str(uuid4())
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'abc'])
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), (['abc'], {}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'abc=def']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'abc': 'def'}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', ':{"json": "abc"}']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([{'json': 'abc'}], {}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'key:={"json": "abc"}']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'key': {'json': 'abc'}}))

        id = str(uuid4())
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', '%1, 2'])
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([(1, 2)], {}))

        id = str(uuid4())
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', '%None'])
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([None], {}))

        id = str(uuid4())
        result = runner.invoke(main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', '%True'])
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([True], {}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'key%=(1, 2)']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'key': (1, 2)}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'key%={"foo": True}']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'key': {"foo": True}}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', '@tests/test.json']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([open('tests/test.json', 'r').read()], {}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'key=@tests/test.json']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'key': open('tests/test.json', 'r').read()}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', ':@tests/test.json']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([json.loads(open('tests/test.json', 'r').read())], {}))

        id = str(uuid4())
        result = runner.invoke(
            main, ['enqueue', '-u', self.redis_url, '--job-id', id, 'tests.fixtures.echo', 'key:=@tests/test.json']
        )
        self.assert_normal_execution(result)
        job = Job.fetch(id, connection=self.connection)
        self.assertEqual((job.args, job.kwargs), ([], {'key': json.loads(open('tests/test.json', 'r').read())}))


class WorkerPoolCLITestCase(CLITestCase):
    def test_worker_pool_burst_and_num_workers(self):
        """rq worker-pool -u <url> -b -n 3"""
        runner = CliRunner()
        result = runner.invoke(main, ['worker-pool', '-u', self.redis_url, '-b', '-n', '3'])
        self.assert_normal_execution(result)

    def test_serializer_and_queue_argument(self):
        """rq worker-pool foo bar -u <url> -b"""
        queue = Queue('foo', connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(say_hello, 'Hello')
        queue = Queue('bar', connection=self.connection, serializer=JSONSerializer)
        job_2 = queue.enqueue(say_hello, 'Hello')
        runner = CliRunner()
        runner.invoke(
            main,
            ['worker-pool', 'foo', 'bar', '-u', self.redis_url, '-b', '--serializer', 'rq.serializers.JSONSerializer'],
        )
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        self.assertEqual(job_2.get_status(refresh=True), JobStatus.FINISHED)

    def test_worker_class_argument(self):
        """rq worker-pool -u <url> -b --worker-class rq.Worker"""
        runner = CliRunner()
        result = runner.invoke(main, ['worker-pool', '-u', self.redis_url, '-b', '--worker-class', 'rq.Worker'])
        self.assert_normal_execution(result)
        result = runner.invoke(
            main, ['worker-pool', '-u', self.redis_url, '-b', '--worker-class', 'rq.worker.SimpleWorker']
        )
        self.assert_normal_execution(result)

        # This one fails because the worker class doesn't exist
        result = runner.invoke(
            main, ['worker-pool', '-u', self.redis_url, '-b', '--worker-class', 'rq.worker.NonExistantWorker']
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_job_class_argument(self):
        """rq worker-pool -u <url> -b --job-class rq.job.Job"""
        runner = CliRunner()
        result = runner.invoke(main, ['worker-pool', '-u', self.redis_url, '-b', '--job-class', 'rq.job.Job'])
        self.assert_normal_execution(result)

        # This one fails because Job class doesn't exist
        result = runner.invoke(
            main, ['worker-pool', '-u', self.redis_url, '-b', '--job-class', 'rq.job.NonExistantJob']
        )
        self.assertNotEqual(result.exit_code, 0)

    def test_worker_pool_logging_options(self):
        """--quiet and --verbose logging options are supported"""
        runner = CliRunner()
        args = ['worker-pool', '-u', self.redis_url, '-b']
        result = runner.invoke(main, args + ['--verbose'])
        self.assert_normal_execution(result)
        result = runner.invoke(main, args + ['--quiet'])
        self.assert_normal_execution(result)

        # --quiet and --verbose are mutually exclusive
        result = runner.invoke(main, args + ['--quiet', '--verbose'])
        self.assertNotEqual(result.exit_code, 0)
