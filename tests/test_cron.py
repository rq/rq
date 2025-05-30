import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import patch

from rq import Queue, utils
from rq.cron import CronJob, CronScheduler, _job_data_registry
from tests import RQTestCase
from tests.fixtures import div_by_zero, do_nothing, say_hello


class BreakLoop(Exception):
    pass


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
            meta=meta,
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
            f'Next run time {next_run_time} not within expected range',
        )

    def test_should_run(self):
        """Test should_run method logic"""

        # Job with interval that has not run yet always returns True
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=3600)
        self.assertTrue(cron_job.should_run())

        # Job with future next_run_time should not run yet
        cron_job.latest_run_time = utils.now() - timedelta(seconds=5)
        cron_job.next_run_time = utils.now() + timedelta(minutes=5)
        self.assertFalse(cron_job.should_run())

        # Job with past next_run_time should run
        cron_job.next_run_time = utils.now() - timedelta(seconds=5)
        self.assertTrue(cron_job.should_run())

    def test_enqueue(self):
        """Test that enqueue correctly creates a job in the queue"""
        cron_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=('Hello',),
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


class TestCronScheduler(RQTestCase):
    """Tests for the Cron class"""

    def setUp(self):
        super().setUp()
        # No default self.cron instance needed here anymore, create it in tests
        self.queue_name = 'default'
        # Ensure clean global registry before each test method in this class
        _job_data_registry.clear()

    def test_register_job(self):
        """Test registering jobs with different configurations"""
        cron = CronScheduler(connection=self.connection)  # Create instance for test

        # Register job with no interval (manual execution only)
        cron_job_manual = cron.register(  # Renamed variable
            func=say_hello, queue_name=self.queue_name
        )

        self.assertIsInstance(cron_job_manual, CronJob)
        self.assertEqual(cron_job_manual.func, say_hello)
        self.assertIsNone(cron_job_manual.interval)

        # Register job with an interval
        interval = 60
        cron_job_interval = cron.register(  # Renamed variable
            func=say_hello, queue_name=self.queue_name, args=('Periodic job',), interval=interval
        )

        self.assertEqual(cron_job_interval.interval, interval)
        self.assertEqual(cron_job_interval.args, ('Periodic job',))

        # Verify jobs are in the cron instance's registry
        registered_jobs = cron.get_jobs()
        self.assertEqual(len(registered_jobs), 2)
        self.assertIn(cron_job_manual, registered_jobs)
        self.assertIn(cron_job_interval, registered_jobs)  # Check the second job too

    def test_enqueue_jobs(self):
        """Test that enqueue_jobs correctly enqueues jobs that are due to run"""
        cron = CronScheduler(connection=self.connection)  # Create instance for test

        # Register jobs with different intervals
        job1 = cron.register(func=say_hello, queue_name=self.queue_name, args=('Job 1',), interval=60)

        job2 = cron.register(func=do_nothing, queue_name=self.queue_name, interval=120)

        job3 = cron.register(func=say_hello, queue_name=self.queue_name, args=('Job 3',), interval=30)

        # Initially, all jobs should run because latest_run_time is not set
        enqueued_jobs = cron.enqueue_jobs()
        queue = Queue(self.queue_name, connection=self.connection)
        self.assertEqual(len(enqueued_jobs), 3)
        self.assertEqual(len(queue.get_jobs()), 3)

        # Store the run time for job2 to check later it wasn't updated
        self.assertIsNotNone(job2.latest_run_time)  # Ensure it was set by the first enqueue
        job_2_latest_run_time = job2.latest_run_time

        queue.empty()
        # Set job1 and job3 to be due, but not job2
        now_time = utils.now()  # Use consistent name
        # Manually set the next run times for testing `should_run` logic
        # Note: latest_run_time is already set from the previous enqueue
        job1.next_run_time = now_time - timedelta(seconds=5)  # 5 seconds ago
        job2.next_run_time = now_time + timedelta(seconds=30)  # 30 seconds in the future
        job3.next_run_time = now_time - timedelta(seconds=10)  # 10 seconds ago

        # Execute enqueue_jobs()
        enqueued_jobs = cron.enqueue_jobs()

        # Check that only job1 and job3 were enqueued
        self.assertEqual(len(enqueued_jobs), 2)
        self.assertIn(job1, enqueued_jobs)
        self.assertIn(job3, enqueued_jobs)
        self.assertNotIn(job2, enqueued_jobs)

        # Check that jobs were actually created in the queue
        queue_jobs = Queue(self.queue_name, connection=self.connection).get_jobs()
        self.assertEqual(len(queue_jobs), 2)

        # Check that the run times were updated for the enqueued jobs
        self.assertIsNotNone(job1.latest_run_time)
        assert job1.latest_run_time is not None and job_2_latest_run_time is not None
        self.assertTrue(job1.latest_run_time > job_2_latest_run_time)
        self.assertIsNotNone(job1.next_run_time)
        self.assertGreaterEqual(job1.next_run_time, now_time)

        self.assertIsNotNone(job3.latest_run_time)
        self.assertTrue(job3.latest_run_time > job_2_latest_run_time)
        self.assertIsNotNone(job3.next_run_time)
        self.assertGreaterEqual(job3.next_run_time, now_time)

        # job2 should not be updated since it wasn't run
        self.assertEqual(job2.latest_run_time, job_2_latest_run_time)  # Check it wasn't updated
        self.assertEqual(job2.next_run_time, now_time + timedelta(seconds=30))  # Still future time

    @patch('rq.cron.now')
    def test_calculate_sleep_interval(self, mock_now):
        """Tests calculate_sleep_interval across various explicit scenarios."""
        cron = CronScheduler(connection=self.connection)  # Create instance for test
        base_time = datetime(2023, 10, 27, 12, 0, 0)
        mock_now.return_value = base_time

        # No Jobs (directly check _cron_jobs on the instance)
        cron._cron_jobs = []  # Ensure no jobs
        actual_interval = cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60.0)

        # Jobs with no next_run_time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name)
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name)
        job1.next_run_time = None
        job2.next_run_time = None
        cron._cron_jobs = [job1, job2]
        actual_interval = cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60)

        # Future job within max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        # Set run time needed to calculate next run time correctly
        job1.set_run_time(base_time - timedelta(seconds=120 - 35))  # Last run so next is in 35s
        # job1.next_run_time = base_time + timedelta(seconds=35) # Or set directly for simplicity
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_run_time(base_time - timedelta(seconds=180 - 70))  # Last run so next is in 70s
        # job2.next_run_time = base_time + timedelta(seconds=70) # Or set directly
        cron._cron_jobs = [job1, job2]
        actual_interval = cron.calculate_sleep_interval()
        # Get the actual next run times after set_run_time
        self.assertEqual(job1.next_run_time, base_time + timedelta(seconds=35))
        self.assertEqual(job2.next_run_time, base_time + timedelta(seconds=70))
        self.assertAlmostEqual(actual_interval, 35.0)  # Expect float

        # Future job over max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        job1.set_run_time(base_time - timedelta(seconds=120 - 90))  # Last run so next is in 90s
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_run_time(base_time - timedelta(seconds=180 - 120))  # Last run so next is in 120s
        cron._cron_jobs = [job1, job2]
        actual_interval = cron.calculate_sleep_interval()
        self.assertEqual(job1.next_run_time, base_time + timedelta(seconds=90))
        self.assertEqual(job2.next_run_time, base_time + timedelta(seconds=120))
        self.assertEqual(actual_interval, 60)  # Capped at 60

        # Overdue job (should run immediately)
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=60)
        job1.set_run_time(base_time - timedelta(seconds=60 + 10))  # Last run was 70s ago, next was 10s ago
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_run_time(base_time - timedelta(seconds=180 - 20))  # Last run so next is in 20s
        cron._cron_jobs = [job1, job2]
        actual_interval = cron.calculate_sleep_interval()
        self.assertEqual(job1.next_run_time, base_time - timedelta(seconds=10))
        self.assertEqual(job2.next_run_time, base_time + timedelta(seconds=20))
        self.assertEqual(actual_interval, 0)

    def test_register_with_job_options(self):
        """Test registering a job with various job options"""
        cron = CronScheduler(connection=self.connection)  # Create instance for test
        timeout = 180
        result_ttl = 600
        meta = {'purpose': 'testing'}

        cron_job = cron.register(
            func=say_hello, queue_name=self.queue_name, interval=30, timeout=timeout, result_ttl=result_ttl, meta=meta
        )

        self.assertEqual(cron_job.job_options['timeout'], timeout)
        self.assertEqual(cron_job.job_options['result_ttl'], result_ttl)
        self.assertEqual(cron_job.job_options['meta'], meta)

    def test_load_config_from_file_method(self):  # Renamed test
        """Test loading cron configuration using the instance method"""
        # Create a Cron instance first
        cron = CronScheduler(connection=self.connection)

        # Load configuration using the method
        config_file_path = 'tests/cron_config.py'
        cron.load_config_from_file(config_file_path)

        # Check all jobs were registered on this instance
        jobs = cron.get_jobs()
        self.assertEqual(len(jobs), 4)

        # Verify specific job properties (same checks as before)
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

        # Verify the global registry is cleared after loading
        self.assertEqual(len(_job_data_registry), 0)

    def test_cron_config_path_finding_method(self):  # Renamed test
        """Test different ways to load cron configuration files using the instance method"""
        # Ensure the test directory is findable if running tests from elsewhere
        test_dir = os.path.dirname(__file__)
        config_file_rel = 'cron_config.py'
        config_file_abs = os.path.join(test_dir, config_file_rel)

        # Verify the config file exists for the test
        self.assertTrue(os.path.exists(config_file_abs), f'Test config file not found at {config_file_abs}')

        # Test 1: Loading with a direct file path (absolute path)
        cron = CronScheduler(connection=self.connection)
        # _job_data_registry is cleared inside the method, no need here
        cron.load_config_from_file(config_file_abs)
        self.assertEqual(len(cron.get_jobs()), 4, 'Failed loading with absolute path')
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after absolute path load')

        # Test 2: Loading with a module path
        cron = CronScheduler(connection=self.connection)
        cron.load_config_from_file('tests.cron_config')
        self.assertEqual(len(cron.get_jobs()), 4, 'Failed loading with module path')
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after module path load')

        # Test 3: Test error handling with a non-existent path
        cron = CronScheduler(connection=self.connection)
        with self.assertRaises(Exception) as context:  # Expect FileNotFoundError or ImportError
            cron.load_config_from_file('path/does/not/exist.py')
        self.assertIn('not found', str(context.exception).lower())
        self.assertEqual(len(cron.get_jobs()), 0)  # No jobs should be loaded
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after non-existent path error')

        # Test 4: Test error handling with a valid file path but invalid content
        with tempfile.NamedTemporaryFile(suffix='.py', mode='w+', delete=False) as invalid_file:
            # Write some invalid Python content
            invalid_file.write('this is not valid python code :')
            invalid_file_path = invalid_file.name  # Store path before closing
        try:
            with self.assertRaises(Exception) as context:  # Expect ImportError or SyntaxError inside
                cron.load_config_from_file(invalid_file_path)

            self.assertIn('failed to load configuration file', str(context.exception).lower())
            self.assertEqual(len(cron.get_jobs()), 0)  # No jobs should be loaded
            self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after invalid content error')
        finally:
            os.remove(invalid_file_path)  # Clean up temp file

    @patch('rq.cron.time.sleep')
    @patch('rq.cron.CronScheduler.calculate_sleep_interval')
    @patch('rq.cron.CronScheduler.enqueue_jobs')
    def test_start_loop(self, mock_enqueue, mock_calculate_interval, mock_sleep):
        """
        Tests cron.start() loop for both sleeping and non-sleeping scenarios.

        Simulates one iteration where sleep occurs (interval > 0) and one
        iteration where sleep is skipped (interval == 0), then breaks the loop.
        """
        cron = CronScheduler(connection=self.connection)  # Create instance for test

        # Simulate:
        # 1st iteration: Calculate interval > 0 (e.g., 15.5), should sleep.
        # 2nd iteration: Calculate interval == 0, should NOT sleep.
        # 3rd call to calculate_interval: Raise BreakLoop to exit test.
        mock_calculate_interval.side_effect = [15.5, 0, BreakLoop]

        # Enqueue can return an empty list for simplicity in this test
        mock_enqueue.return_value = []

        # Run start() and expect it to break when calculate_interval raises BreakLoop
        with self.assertRaises(BreakLoop):
            cron.start()  # Call start on the instance

        # --- Assertions ---
        self.assertEqual(mock_enqueue.call_count, 3)
        self.assertEqual(
            mock_calculate_interval.call_count, 3, 'calculate_sleep_interval should be called thrice (15.5, 0, raises)'
        )

        self.assertEqual(mock_sleep.call_count, 1, 'time.sleep should be called only once')
        mock_sleep.assert_called_once_with(15.5)  # Verify it was called with the correct interval
