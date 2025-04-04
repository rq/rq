import os
import sys
from datetime import datetime, timedelta
from unittest.mock import call, patch

from rq import Queue
from rq import utils
from rq.cron import Cron, CronJob, load_config
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

    def test_enqueue_jobs(self):
        """Test that enqueue_jobs correctly enqueues jobs that are due to run"""
        # Register jobs with different intervals
        job1 = self.cron.register(
            func=say_hello,
            queue_name=self.queue_name,
            args=("Job 1",),
            interval=60
        )

        job2 = self.cron.register(
            func=do_nothing,
            queue_name=self.queue_name,
            interval=120
        )

        job3 = self.cron.register(
            func=say_hello,
            queue_name=self.queue_name,
            args=("Job 3",),
            interval=30
        )

        # Initially, all jobs should run because latest_run_time is not set
        enqueued_jobs = self.cron.enqueue_jobs()
        queue = Queue(self.queue_name, connection=self.connection)
        self.assertEqual(len(enqueued_jobs), 3)
        self.assertEqual(len(queue.get_jobs()), 3)

        job_2_latest_run_time = job2.latest_run_time

        queue.empty()
        # Set job1 and job3 to be due, but not job2
        now = utils.now()
        job1.next_run_time = now - timedelta(seconds=5)  # 5 seconds ago
        job2.next_run_time = now + timedelta(seconds=30)  # 30 seconds in the future
        job3.next_run_time = now - timedelta(seconds=10)  # 10 seconds ago

        # Execute enqueue_jobs()
        enqueued_jobs = self.cron.enqueue_jobs()

        # Check that only job1 and job3 were enqueued
        self.assertEqual(len(enqueued_jobs), 2)
        self.assertIn(job1, enqueued_jobs)
        self.assertIn(job3, enqueued_jobs)
        self.assertNotIn(job2, enqueued_jobs)

        # Check that jobs were actually created in the queue
        queue_jobs = Queue(self.queue_name, connection=self.connection).get_jobs()
        self.assertEqual(len(queue_jobs), 2)

        # Check that the next_run_time was updated for the enqueued jobs
        self.assertIsNotNone(job1.latest_run_time)
        self.assertGreaterEqual(job1.next_run_time, now)  # Should be about 60 seconds from now

        self.assertIsNotNone(job3.latest_run_time)
        self.assertGreaterEqual(job3.next_run_time, now)  # Should be about 30 seconds from now

        # job2 should not be updated since it wasn't run
        self.assertEqual(job2.next_run_time, now + timedelta(seconds=30))
        self.assertEqual(job2.latest_run_time, job_2_latest_run_time)

    @patch('rq.cron.now')
    def test_calculate_sleep_interval(self, mock_now):
        """Tests calculate_sleep_interval across various explicit scenarios."""

        base_time = datetime(2023, 10, 27, 12, 0, 0)

        # No Jobs
        mock_now.return_value = base_time
        self.cron._cron_jobs = [] # Ensure no jobs
        actual_interval = self.cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60.0)

        # Jobs with no next_run_time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name)
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name)
        job1.next_run_time = None
        job2.next_run_time = None
        self.cron._cron_jobs = [job1, job2]
        actual_interval = self.cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60)

        # Future job within max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        job1.next_run_time = base_time + timedelta(seconds=35) # Next run in 35s
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.next_run_time = base_time + timedelta(seconds=70) # Next run in 70s
        self.cron._cron_jobs = [job1, job2]
        actual_interval = self.cron.calculate_sleep_interval()
        self.assertAlmostEqual(actual_interval, 35)

        # Future job over max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        job1.next_run_time = base_time + timedelta(seconds=90) # Next run in 90s
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.next_run_time = base_time + timedelta(seconds=120) # Next run in 120s
        self.cron._cron_jobs = [job1, job2]
        actual_interval = self.cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60) # Capped at 60

        # Overdue job (should run immediately)
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=60)
        job1.next_run_time = base_time - timedelta(seconds=10) # 10s overdue
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.next_run_time = base_time + timedelta(seconds=20) # Next run in 20s
        self.cron._cron_jobs = [job1, job2]
        actual_interval = self.cron.calculate_sleep_interval()
        self.assertEqual(actual_interval, 0)

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

        # Test 2: Loading with a module path
        if '' not in sys.path:
            sys.path.insert(0, '')  # Make sure current directory is in path

        _job_data_registry.clear()  # Reset registry
        cron3 = load_config('tests.cron_config', self.connection)
        self.assertEqual(len(cron3.get_jobs()), 4)

        # Test 3: Test error handling with a non-existent path
        with self.assertRaises(Exception) as context:
            load_config('path/does/not/exist', self.connection)

        # Test 4: Test error handling with a valid file path but invalid content
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.py', mode='w+') as invalid_file:
            # Write some invalid Python content
            invalid_file.write("this is not valid python code :")
            invalid_file.flush()

            with self.assertRaises(Exception) as context:
                load_config(invalid_file.name, self.connection)

            self.assertIn("Failed to load configuration", str(context.exception))

    @patch('rq.cron.time.sleep')
    @patch('rq.cron.Cron.calculate_sleep_interval')
    @patch('rq.cron.Cron.enqueue_jobs')
    def test_start_loop(self, mock_enqueue, mock_calculate_interval, mock_sleep):
        """
        Tests cron.start() loop for both sleeping and non-sleeping scenarios.

        Simulates one iteration where sleep occurs (interval > 0) and one
        iteration where sleep is skipped (interval == 0), then breaks the loop.
        """
        # --- Mock Configuration ---
        # Simulate:
        # 1st iteration: Calculate interval > 0 (e.g., 15.5), should sleep.
        # 2nd iteration: Calculate interval == 0, should NOT sleep.
        # 3rd call to calculate_interval: Raise BreakLoop to exit test.
        mock_calculate_interval.side_effect = [15.5, 0, BreakLoop]

        # Enqueue can return an empty list for simplicity in this test
        mock_enqueue.return_value = []

        # Sleep mock doesn't need a complex side_effect here,
        # we verify its calls based on calculate_interval's return.

        # Run start() and expect it to break when calculate_interval raises BreakLoop
        with self.assertRaises(BreakLoop):
            self.cron.start()

        # --- Assertions ---
        # `cron.start()` loop executes `enqueue_jobs()` at the beginning of each iteration.
        # Mock setup (`calculate_sleep_interval.side_effect`) are configured to simulate:
        # 1. Iteration 1: enqueue_jobs() runs, calculate_sleep_interval() returns 15.5, sleep() runs.
        # 2. Iteration 2: enqueue_jobs() runs, calculate_sleep_interval() returns 0, sleep() is skipped.
        # 3. Iteration 3: enqueue_jobs() runs, calculate_sleep_interval() raises BreakLoop, loop exits.
        # Therefore, enqueue_jobs() is called 3 times before the loop terminates.
        self.assertEqual(mock_enqueue.call_count, 3)
        self.assertEqual(mock_calculate_interval.call_count, 3, "calculate_sleep_interval should be called thrice (15.5, 0, raises)")

        # time.sleep() should have been called once, during the first iteration.
        self.assertEqual(mock_sleep.call_count, 1, "time.sleep should be called only once")
        mock_sleep.assert_called_once_with(15.5) # Verify it was called with the correct interval
