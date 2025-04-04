import os
import sys
from datetime import datetime, timedelta

from rq import Queue
from rq import utils
from rq.cron import Cron, CronJob, load_config
from rq.job import Job
from tests import RQTestCase
from tests.fixtures import div_by_zero, do_nothing, say_hello


class TestCronJob(RQTestCase):
    """Tests for the CronJob class"""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)

    def test_cron_job_initialization(self):
        """CronJob correctly initializes with the provided parameters"""
        # Test with minimum required parameters
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name)
        self.assertEqual(cron_job.func, say_hello)
        self.assertEqual(cron_job.args, ())
        self.assertEqual(cron_job.kwargs, {})
        self.assertIsNone(cron_job.interval)
        self.assertEqual(cron_job.queue_name, self.queue.name)

        # Test with all parameters
        args = (1, 2, 3)
        kwargs = {'name': 'Test'}
        interval = 60
        timeout = 180
        result_ttl = 600
        ttl = 300
        failure_ttl = 400
        meta = {'key': 'value'}

        cron_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=args,
            kwargs=kwargs,
            interval=interval,
            timeout=timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            meta=meta
        )

        self.assertEqual(cron_job.func, say_hello)
        self.assertEqual(cron_job.args, args)
        self.assertEqual(cron_job.kwargs, kwargs)
        self.assertEqual(cron_job.interval, interval)
        self.assertEqual(cron_job.queue_name, self.queue.name)

        # Check job options dict
        self.assertEqual(cron_job.job_options['timeout'], timeout)
        self.assertEqual(cron_job.job_options['result_ttl'], result_ttl)
        self.assertEqual(cron_job.job_options['ttl'], ttl)
        self.assertEqual(cron_job.job_options['failure_ttl'], failure_ttl)
        self.assertEqual(cron_job.job_options['meta'], meta)

    def test_get_next_run_time(self):
        """Test that get_next_run_time correctly calculates the next run time"""
        # Test with no interval
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=None)
        self.assertEqual(cron_job.get_next_run_time(), datetime.max)

        # Test with a specific interval
        interval = 60  # 60 seconds
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=interval)

        now = utils.now()
        cron_job.set_run_time(now)
        next_run_time = cron_job.get_next_run_time()

        # Check that next_run_time is about interval seconds from now
        # Allow 2 seconds tolerance for test execution time
        self.assertTrue(
            now + timedelta(seconds=interval - 5) <= next_run_time <= now + timedelta(seconds=interval + 5),
            f"Next run time {next_run_time} not within expected range"
        )

    def test_should_run(self):
        """Test should_run method logic"""
        # Job with no interval should not run automatically
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=None)
        self.assertFalse(cron_job.should_run())

        # Job with future next_run_time should not run yet
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=3600)  # 1 hour
        cron_job.next_run_time = utils.now() + timedelta(minutes=5)
        self.assertFalse(cron_job.should_run())

        # Job with past next_run_time should run
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=60)
        cron_job.next_run_time = utils.now() - timedelta(seconds=5)
        self.assertTrue(cron_job.should_run())

    def test_enqueue(self):
        """Test that enqueue correctly creates a job in the queue"""
        cron_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=("Hello",),
            interval=60,
        )
        job = cron_job.enqueue(self.connection)

        # Fetch job from queue and verify
        jobs = self.queue.get_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].id, job.id)

    def test_set_run_time(self):
        """Test that set_run_time correctly sets latest run time and updates next run time"""
        # Test with no interval
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=None)
        test_time = utils.now()
        cron_job.set_run_time(test_time)

        # Check latest_run_time is set correctly
        self.assertEqual(cron_job.latest_run_time, test_time)

        # Since interval is None, next_run_time should not be set
        self.assertIsNone(cron_job.next_run_time)

        # Test with an interval
        interval = 60  # 60 seconds
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=interval)
        cron_job.set_run_time(test_time)

        # Check latest_run_time is set correctly
        self.assertEqual(cron_job.latest_run_time, test_time)

        # Check that next_run_time is calculated correctly
        next_run_time = test_time + timedelta(seconds=interval)
        self.assertEqual(cron_job.next_run_time, next_run_time)

        # Test updating the run time
        test_time = test_time + timedelta(seconds=30)
        cron_job.set_run_time(test_time)

        # Check latest_run_time is updated
        self.assertEqual(cron_job.latest_run_time, test_time)

        # Check that next_run_time is recalculated
        new_expected_next_run = test_time + timedelta(seconds=interval)
        self.assertEqual(cron_job.next_run_time, new_expected_next_run)


class TestCron(RQTestCase):
    """Tests for the Cron class"""

    def setUp(self):
        super().setUp()
        self.cron = Cron(connection=self.connection)
        self.queue_name = "default"

    def test_register_job(self):
        """Test registering jobs with different configurations"""
        # Register job with no interval (manual execution only)
        cron_job = self.cron.register(
            func=say_hello,
            queue_name=self.queue_name
        )

        self.assertIsInstance(cron_job, CronJob)
        self.assertEqual(cron_job.func, say_hello)
        self.assertIsNone(cron_job.interval)

        # Register job with an interval
        interval = 60
        cron_job = self.cron.register(
            func=say_hello,
            queue_name=self.queue_name,
            args=("Periodic job",),
            interval=interval
        )

        self.assertEqual(cron_job.interval, interval)
        self.assertEqual(cron_job.args, ("Periodic job",))

        # Verify jobs are in the cron registry
        registered_jobs = self.cron.get_jobs()
        self.assertEqual(len(registered_jobs), 2)
        self.assertIn(cron_job, registered_jobs)
        self.assertIn(cron_job, registered_jobs)

    def test_register_with_job_options(self):
        """Test registering a job with various job options"""
        timeout = 180
        result_ttl = 600
        meta = {'purpose': 'testing'}

        cron_job = self.cron.register(
            func=say_hello,
            queue_name=self.queue_name,
            interval=30,
            timeout=timeout,
            result_ttl=result_ttl,
            meta=meta
        )

        self.assertEqual(cron_job.job_options['timeout'], timeout)
        self.assertEqual(cron_job.job_options['result_ttl'], result_ttl)
        self.assertEqual(cron_job.job_options['meta'], meta)

    def test_load_config(self):
        """Test loading cron configuration from a file"""
        # Load configuration
        cron = load_config('tests/cron_config.py', self.connection)

        # Check all jobs were registered
        jobs = cron.get_jobs()
        self.assertEqual(len(jobs), 4)

        # Verify specific job properties
        job_functions = [job.func for job in jobs]
        job_intervals = [job.interval for job in jobs]

        self.assertIn(say_hello, job_functions)
        self.assertIn(div_by_zero, job_functions)
        self.assertIn(do_nothing, job_functions)
        self.assertIn(30, job_intervals)
        self.assertIn(180, job_intervals)

        # Find job with kwargs
        kwargs_job = next((job for job in jobs if job.kwargs.get('name') == 'RQ Cron'), None)
        self.assertIsNotNone(kwargs_job)
        self.assertEqual(kwargs_job.interval, 120)

        # Find job with args
        args_job = next((job for job in jobs if job.args == (10,)), None)
        self.assertIsNotNone(args_job)
        self.assertEqual(args_job.func, div_by_zero)

    def test_cron_config_path_finding(self):
        """Test different ways to load cron configuration files using path finding"""
        # Setup: Make sure we have a clean registry before each test
        from rq.cron import _job_data_registry

        # Test 1: Loading with a direct file path (absolute path)
        abs_path = os.path.abspath('tests/cron_config.py')
        _job_data_registry.clear()  # Reset registry
        cron1 = load_config(abs_path, self.connection)
        self.assertEqual(len(cron1.get_jobs()), 4)

        # Test 2: Loading with a file path without .py extension
        _job_data_registry.clear()  # Reset registry
        cron2 = load_config('tests/cron_config', self.connection)
        self.assertEqual(len(cron2.get_jobs()), 4)

        # Test 3: Loading with a module path
        if '' not in sys.path:
            sys.path.insert(0, '')  # Make sure current directory is in path

        _job_data_registry.clear()  # Reset registry
        cron3 = load_config('tests.cron_config', self.connection)
        self.assertEqual(len(cron3.get_jobs()), 4)

        # Test 4: Test error handling with a non-existent path
        with self.assertRaises(Exception) as context:
            load_config('path/does/not/exist', self.connection)

        # Test 5: Test error handling with a valid file path but invalid content
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.py', mode='w+') as invalid_file:
            # Write some invalid Python content
            invalid_file.write("this is not valid python code :")
            invalid_file.flush()

            with self.assertRaises(Exception) as context:
                load_config(invalid_file.name, self.connection)

            self.assertIn("Failed to load cron configuration", str(context.exception))
