import json
import os
import signal
import socket
import tempfile
import time
from datetime import datetime, timedelta, timezone
from multiprocessing import Process
from typing import cast
from unittest.mock import patch

from redis import Redis

from rq import Queue, utils
from rq.cron import CronJob, CronScheduler, _job_data_registry
from rq.cron_scheduler_registry import get_keys, get_registry_key
from rq.exceptions import SchedulerNotFound
from tests import RQTestCase
from tests.fixtures import div_by_zero, do_nothing, say_hello


class BreakLoop(Exception):
    pass


def run_scheduler(redis_connection_kwargs):
    """Target function to run the scheduler in a separate process."""
    scheduler = CronScheduler(connection=Redis(**redis_connection_kwargs))
    # Register a job that runs every second to keep the scheduler busy
    scheduler.register(do_nothing, 'default', interval=1)
    scheduler.start()


class TestCronScheduler(RQTestCase):
    """Tests for the Cron class"""

    def setUp(self):
        super().setUp()
        # No default self.cron instance needed here anymore, create it in tests
        self.queue_name = 'default'
        # Ensure clean global registry before each test method in this class
        _job_data_registry.clear()

    def test_scheduler_tracking_attributes(self):
        """Test that CronScheduler tracks hostname, pid, and config_file"""
        scheduler = CronScheduler(connection=self.connection)

        # Test initial values
        self.assertEqual(scheduler.hostname, socket.gethostname())
        self.assertEqual(scheduler.pid, os.getpid())
        self.assertFalse(scheduler.config_file)

        # Test config_file is set when loading config
        config_file_path = 'tests/cron_config.py'
        scheduler.load_config_from_file(config_file_path)
        self.assertEqual(scheduler.config_file, config_file_path)

    def test_register_job(self):
        """Test registering jobs with different configurations"""
        scheduler = CronScheduler(connection=self.connection)

        # Test with cron expression
        cron_job = scheduler.register(func=say_hello, queue_name=self.queue_name, cron='0 9 * * *')
        self.assertIsInstance(cron_job, CronJob)
        self.assertEqual(cron_job.func, say_hello)
        self.assertIsNone(cron_job.interval)
        self.assertEqual(cron_job.cron, '0 9 * * *')

        # Test with interval
        interval_job = scheduler.register(
            func=say_hello, queue_name=self.queue_name, args=('Periodic job',), interval=60
        )
        self.assertEqual(interval_job.interval, 60)
        self.assertEqual(interval_job.args, ('Periodic job',))

        # Verify jobs are registered
        registered_jobs = scheduler.get_jobs()
        self.assertEqual(len(registered_jobs), 2)
        self.assertIn(cron_job, registered_jobs)

        # Test validation: both interval and cron raises error
        with self.assertRaises(ValueError):
            scheduler.register(func=say_hello, queue_name=self.queue_name, interval=60, cron='0 9 * * *')

        # Test validation: neither interval nor cron raises error
        with self.assertRaises(ValueError):
            scheduler.register(func=say_hello, queue_name=self.queue_name)

    def test_enqueue_jobs(self):
        """Test that enqueue_jobs correctly enqueues jobs that are due to run"""
        scheduler = CronScheduler(connection=self.connection)  # Create instance for test

        # Register jobs with different intervals
        job1 = scheduler.register(func=say_hello, queue_name=self.queue_name, args=('Job 1',), interval=60)

        job2 = scheduler.register(func=do_nothing, queue_name=self.queue_name, interval=120)

        job3 = scheduler.register(func=say_hello, queue_name=self.queue_name, args=('Job 3',), interval=30)

        # Initially, all jobs should run because latest_enqueue_time is not set
        enqueued_jobs = scheduler.enqueue_jobs()
        queue = Queue(self.queue_name, connection=self.connection)
        self.assertEqual(len(enqueued_jobs), 3)
        self.assertEqual(len(queue.get_jobs()), 3)

        self.assertIsNotNone(job2.latest_enqueue_time)
        job_2_latest_enqueue_time = job2.latest_enqueue_time

        queue.empty()
        # Set job1 and job3 to be due, but not job2
        now = utils.now()
        # Manually set the next run times for testing `should_run` logic
        # Note: latest_enqueue_time is already set from the previous enqueue
        job1.next_enqueue_time = now - timedelta(seconds=5)
        job2.next_enqueue_time = now + timedelta(seconds=30)
        job3.next_enqueue_time = now - timedelta(seconds=10)

        # Execute enqueue_jobs()
        enqueued_jobs = scheduler.enqueue_jobs()

        # Check that only job1 and job3 were enqueued
        self.assertEqual(len(enqueued_jobs), 2)
        self.assertIn(job1, enqueued_jobs)
        self.assertIn(job3, enqueued_jobs)
        self.assertNotIn(job2, enqueued_jobs)

        # Check that jobs were actually created in the queue
        queue_jobs = Queue(self.queue_name, connection=self.connection).get_jobs()
        self.assertEqual(len(queue_jobs), 2)

        # Check that the run times were updated for the enqueued jobs
        assert job1.latest_enqueue_time is not None and job_2_latest_enqueue_time is not None
        self.assertTrue(job1.latest_enqueue_time > job_2_latest_enqueue_time)
        self.assertIsNotNone(job1.next_enqueue_time)
        self.assertGreaterEqual(job1.next_enqueue_time, now)

        self.assertIsNotNone(job3.latest_enqueue_time)
        self.assertTrue(job3.latest_enqueue_time > job_2_latest_enqueue_time)
        self.assertIsNotNone(job3.next_enqueue_time)
        self.assertGreaterEqual(job3.next_enqueue_time, now)

        # job2 should not be updated since it wasn't run
        self.assertEqual(job2.latest_enqueue_time, job_2_latest_enqueue_time)
        self.assertEqual(job2.next_enqueue_time, now + timedelta(seconds=30))

    @patch('rq.cron.now')
    def test_calculate_sleep_interval(self, mock_now):
        """Tests calculate_sleep_interval across various explicit scenarios."""
        scheduler = CronScheduler(connection=self.connection)
        base_time = datetime(2023, 10, 27, 12, 0, 0)
        mock_now.return_value = base_time

        # No Jobs (directly check _cron_jobs on the instance)
        scheduler._cron_jobs = []
        actual_interval = scheduler.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60)

        # Jobs with no next_enqueue_time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=60)
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=120)
        job1.next_enqueue_time = None
        job2.next_enqueue_time = None
        scheduler._cron_jobs = [job1, job2]
        actual_interval = scheduler.calculate_sleep_interval()
        self.assertEqual(actual_interval, 60)

        # Future job within max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        # Set run time needed to calculate next run time correctly
        job1.set_enqueue_time(base_time - timedelta(seconds=120 - 35))  # Last run so next is in 35s
        # job1.next_enqueue_time = base_time + timedelta(seconds=35) # Or set directly for simplicity
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_enqueue_time(base_time - timedelta(seconds=180 - 70))  # Last run so next is in 70s
        # job2.next_enqueue_time = base_time + timedelta(seconds=70) # Or set directly
        scheduler._cron_jobs = [job1, job2]
        actual_interval = scheduler.calculate_sleep_interval()
        # Get the actual next run times after set_enqueue_time
        self.assertEqual(job1.next_enqueue_time, base_time + timedelta(seconds=35))
        self.assertEqual(job2.next_enqueue_time, base_time + timedelta(seconds=70))
        self.assertAlmostEqual(actual_interval, 35)

        # Future job over max sleep time
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=120)
        job1.set_enqueue_time(base_time - timedelta(seconds=120 - 90))  # Last run so next is in 90s
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_enqueue_time(base_time - timedelta(seconds=180 - 120))  # Last run so next is in 120s
        scheduler._cron_jobs = [job1, job2]
        actual_interval = scheduler.calculate_sleep_interval()
        self.assertEqual(job1.next_enqueue_time, base_time + timedelta(seconds=90))
        self.assertEqual(job2.next_enqueue_time, base_time + timedelta(seconds=120))
        self.assertEqual(actual_interval, 60)  # Capped at 60

        # Overdue job (should run immediately)
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, interval=60)
        job1.set_enqueue_time(base_time - timedelta(seconds=60 + 10))  # Last run was 70s ago, next was 10s ago
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, interval=180)
        job2.set_enqueue_time(base_time - timedelta(seconds=180 - 20))  # Last run so next is in 20s
        scheduler._cron_jobs = [job1, job2]
        actual_interval = scheduler.calculate_sleep_interval()
        self.assertEqual(job1.next_enqueue_time, base_time - timedelta(seconds=10))
        self.assertEqual(job2.next_enqueue_time, base_time + timedelta(seconds=20))
        self.assertEqual(actual_interval, 0)

    def test_register_with_job_options(self):
        """Test registering a job with various job options"""
        scheduler = CronScheduler(connection=self.connection)  # Create instance for test
        timeout = 180
        result_ttl = 600
        meta = {'purpose': 'testing'}

        cron_job = scheduler.register(
            func=say_hello,
            queue_name=self.queue_name,
            interval=30,
            job_timeout=timeout,
            result_ttl=result_ttl,
            meta=meta,
        )

        self.assertEqual(cron_job.job_options['job_timeout'], timeout)
        self.assertEqual(cron_job.job_options['result_ttl'], result_ttl)
        self.assertEqual(cron_job.job_options['meta'], meta)

    def test_load_config_from_file_method(self):  # Renamed test
        """Test loading cron configuration using the instance method"""
        # Create a Cron instance first
        scheduler = CronScheduler(connection=self.connection)

        # Load configuration using the method
        config_file_path = 'tests/cron_config.py'
        scheduler.load_config_from_file(config_file_path)

        # Check all jobs were registered on this instance
        jobs = scheduler.get_jobs()
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
        scheduler = CronScheduler(connection=self.connection)
        # _job_data_registry is cleared inside the method, no need here
        scheduler.load_config_from_file(config_file_abs)
        self.assertEqual(len(scheduler.get_jobs()), 4, 'Failed loading with absolute path')
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after absolute path load')

        # Test 2: Loading with a module path
        scheduler = CronScheduler(connection=self.connection)
        scheduler.load_config_from_file('tests.cron_config')
        self.assertEqual(len(scheduler.get_jobs()), 4, 'Failed loading with module path')
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after module path load')

        # Test 3: Test error handling with a non-existent path
        scheduler = CronScheduler(connection=self.connection)
        with self.assertRaises(Exception):  # Expect FileNotFoundError or ImportError
            scheduler.load_config_from_file('path/does/not/exist.py')
        self.assertEqual(len(scheduler.get_jobs()), 0)  # No jobs should be loaded
        self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after non-existent path error')

        # Test 4: Test error handling with a valid file path but invalid content
        with tempfile.NamedTemporaryFile(suffix='.py', mode='w+', delete=False) as invalid_file:
            # Write some invalid Python content
            invalid_file.write('this is not valid python code :')
            invalid_file_path = invalid_file.name  # Store path before closing
        try:
            with self.assertRaises(Exception):  # Expect ImportError or SyntaxError inside
                scheduler.load_config_from_file(invalid_file_path)

            self.assertEqual(len(scheduler.get_jobs()), 0)  # No jobs should be loaded
            self.assertEqual(len(_job_data_registry), 0, 'Registry not cleared after invalid content error')
        finally:
            os.remove(invalid_file_path)  # Clean up temp file

    @patch('rq.cron.time.sleep')
    @patch('rq.cron.CronScheduler.calculate_sleep_interval')
    @patch('rq.cron.CronScheduler.enqueue_jobs')
    def test_start_loop(self, mock_enqueue, mock_calculate_interval, mock_sleep):
        """
        Tests scheduler.start() loop for both sleeping and non-sleeping scenarios.

        Simulates one iteration where sleep occurs (interval > 0) and one
        iteration where sleep is skipped (interval == 0), then breaks the loop.
        """
        scheduler = CronScheduler(connection=self.connection)  # Create instance for test

        # Simulate:
        # 1st iteration: Calculate interval > 0 (e.g., 15.5), should sleep.
        # 2nd iteration: Calculate interval == 0, should NOT sleep.
        # 3rd call to calculate_interval: Raise BreakLoop to exit test.
        mock_calculate_interval.side_effect = [15.5, 0, BreakLoop]

        # Enqueue can return an empty list for simplicity in this test
        mock_enqueue.return_value = []

        # Run start() and expect it to break when calculate_interval raises BreakLoop
        with self.assertRaises(BreakLoop):
            scheduler.start()  # Call start on the instance

        # --- Assertions ---
        self.assertEqual(mock_enqueue.call_count, 3)
        self.assertEqual(
            mock_calculate_interval.call_count, 3, 'calculate_sleep_interval should be called thrice (15.5, 0, raises)'
        )

        self.assertEqual(mock_sleep.call_count, 1, 'time.sleep should be called only once')
        mock_sleep.assert_called_once_with(15.5)  # Verify it was called with the correct interval

    def test_enqueue_jobs_with_cron_strings(self):
        """Test that scheduler.register correctly handles cron-scheduled jobs"""
        scheduler = CronScheduler(connection=self.connection)

        # Register a job that should run every minute
        job1 = scheduler.register(
            func=say_hello,
            queue_name=self.queue_name,
            cron='* * * * *',  # Every minute
        )

        # Register a job that should run at a specific time in the future
        job2 = scheduler.register(
            func=do_nothing,
            queue_name=self.queue_name,
            cron='0 12 * * *',  # Daily at noon
        )

        # Initially, cron jobs should NOT run immediately (crontab behavior)
        enqueued_jobs = scheduler.enqueue_jobs()
        queue = Queue(self.queue_name, connection=self.connection)
        self.assertEqual(len(enqueued_jobs), 0)  # No jobs should be enqueued initially
        self.assertEqual(len(queue.get_jobs()), 0)

        # Verify next_enqueue_time was initialized but jobs didn't run
        self.assertIsNone(job1.latest_enqueue_time)
        self.assertIsNotNone(job1.next_enqueue_time)
        self.assertIsNone(job2.latest_enqueue_time)
        self.assertIsNotNone(job2.next_enqueue_time)

    @patch('rq.cron.now')
    def test_cron_jobs_run_when_scheduled_time_arrives(self, mock_now):
        """Test that cron jobs run when their scheduled time arrives"""
        scheduler = CronScheduler(connection=self.connection)

        # Set current time to 8:59 AM (1 minute before 9 AM)
        current_time = datetime(2023, 10, 27, 8, 59, 0)
        mock_now.return_value = current_time

        # Register job scheduled for 9 AM daily
        job = scheduler.register(
            func=say_hello,
            queue_name=self.queue_name,
            cron='0 9 * * *',  # 9 AM daily
        )

        # Initially should not run (1 minute before schedule)
        enqueued_jobs = scheduler.enqueue_jobs()
        self.assertEqual(len(enqueued_jobs), 0)
        self.assertIsNone(job.latest_enqueue_time)
        self.assertIsNotNone(job.next_enqueue_time)

        # Now advance time to exactly 9 AM
        scheduled_time = datetime(2023, 10, 27, 9, 0, 0)
        mock_now.return_value = scheduled_time

        # Now the job should run
        enqueued_jobs = scheduler.enqueue_jobs()
        queue = Queue(self.queue_name, connection=self.connection)

        # Verify job was enqueued
        self.assertEqual(len(enqueued_jobs), 1)
        self.assertEqual(len(queue.get_jobs()), 1)
        self.assertIn(job, enqueued_jobs)

        # Verify job was marked as run and next run time updated
        self.assertIsNotNone(job.latest_enqueue_time)
        self.assertEqual(job.latest_enqueue_time, scheduled_time)
        self.assertIsNotNone(job.next_enqueue_time)
        # Next run should be tomorrow at 9 AM
        expected_next_run = datetime(2023, 10, 28, 9, 0, 0)
        self.assertEqual(job.next_enqueue_time, expected_next_run)

        # Verify the actual queued job has correct function
        queued_job = queue.get_jobs()[0]
        self.assertEqual(queued_job.func_name, 'tests.fixtures.say_hello')

    @patch('rq.cron.now')
    def test_multiple_cron_jobs_selective_execution(self, mock_now):
        """Test that only cron jobs whose time has arrived are executed"""
        scheduler = CronScheduler(connection=self.connection)

        # Set current time to 8:15 AM
        mock_now.return_value = datetime(2023, 10, 27, 8, 15, 0)

        # Register multiple jobs with different schedules
        job_9am = scheduler.register(
            func=say_hello,
            queue_name=self.queue_name,
            args=('9 AM job',),
            cron='0 9 * * *',  # 9 AM daily
        )

        job_10am = scheduler.register(
            func=do_nothing,
            queue_name=self.queue_name,
            cron='0 10 * * *',  # 10 AM daily
        )

        job_every_30_min = scheduler.register(
            func=say_hello,
            queue_name=self.queue_name,
            cron='*/30 * * * *',  # Every 30 minutes
        )

        # At 8:30 AM, only the 30-minute job should be ready
        # (assuming it last ran at 8:00 AM or this is its first run at 8:30)
        mock_now.return_value = datetime(2023, 10, 27, 8, 30, 0)
        enqueued_jobs = scheduler.enqueue_jobs()

        # Only the 30-minute job should run (as it matches the current time)
        self.assertEqual(enqueued_jobs, [job_every_30_min])

        # Now advance to 9:00 AM
        mock_now.return_value = datetime(2023, 10, 27, 9, 0, 0)
        enqueued_jobs = scheduler.enqueue_jobs()

        # Now the 9 AM job should also run, but not the 10 AM job
        self.assertEqual(len(enqueued_jobs), 2)
        self.assertIn(job_every_30_min, enqueued_jobs)
        self.assertIn(job_9am, enqueued_jobs)
        self.assertNotIn(job_10am, enqueued_jobs)

    @patch('rq.cron.now')
    def test_calculate_sleep_interval_with_cron_jobs(self, mock_now):
        """Test calculate_sleep_interval with cron-scheduled jobs"""
        scheduler = CronScheduler(connection=self.connection)
        # Set current time to 8:58 AM
        mock_now.return_value = datetime(2023, 10, 27, 8, 58, 0)

        # Create jobs that will have next_enqueue_time set based on the mock time
        # Job 1: scheduled for every minute (next run at 8:59 AM, 1 minute away)
        job1 = CronJob(func=say_hello, queue_name=self.queue_name, cron='* * * * *')

        # Job 2: scheduled for 9:05 AM (7 minutes away)
        job2 = CronJob(func=do_nothing, queue_name=self.queue_name, cron='5 9 * * *')

        scheduler._cron_jobs = [job1, job2]
        actual_interval = scheduler.calculate_sleep_interval()

        # Should sleep for 1 minute (60 seconds) until the first job
        self.assertEqual(actual_interval, 60.0)

        # Test with a closer time - 30 seconds before next job
        mock_now.return_value = datetime(2023, 10, 27, 8, 58, 30)

        actual_interval = scheduler.calculate_sleep_interval()
        # Should sleep for 30 seconds (not capped at 60)
        self.assertEqual(actual_interval, 30.0)

    def test_mixed_interval_and_cron_jobs(self):
        """Test that interval-based and cron-based jobs can coexist"""
        scheduler = CronScheduler(connection=self.connection)

        # Register interval-based job
        interval_job = scheduler.register(func=say_hello, queue_name=self.queue_name, interval=120)

        # Register cron-based job
        cron_job = scheduler.register(func=do_nothing, queue_name=self.queue_name, cron='*/5 * * * *')

        # Verify both jobs are registered
        registered_jobs = scheduler.get_jobs()
        self.assertEqual(len(registered_jobs), 2)
        self.assertIn(interval_job, registered_jobs)
        self.assertIn(cron_job, registered_jobs)

        # Verify their properties
        self.assertEqual(interval_job.interval, 120)
        self.assertIsNone(interval_job.cron)
        self.assertEqual(cron_job.cron, '*/5 * * * *')
        self.assertIsNone(cron_job.interval)

        # Interval job should run immediately, cron job should wait for schedule
        self.assertTrue(interval_job.should_run())  # Interval jobs run immediately
        self.assertFalse(cron_job.should_run())  # Cron jobs wait for their schedule

    def test_cron_scheduler_to_dict(self):
        """Test that CronScheduler serializes to dictionary with jobs"""
        scheduler = CronScheduler(connection=self.connection, name='test-scheduler')
        scheduler.config_file = 'test_config.py'

        # Test basic fields
        data = scheduler.to_dict()
        self.assertEqual(data['hostname'], scheduler.hostname)
        self.assertEqual(data['pid'], str(scheduler.pid))
        self.assertEqual(data['name'], 'test-scheduler')
        self.assertEqual(data['config_file'], 'test_config.py')
        self.assertIn('created_at', data)
        self.assertIn('cron_jobs', data)

        # Test with jobs
        scheduler.register(say_hello, 'default', interval=60)
        scheduler.register(do_nothing, 'high', cron='0 * * * *')
        scheduler.register(div_by_zero, 'low', interval=120, job_timeout=10)

        data = scheduler.to_dict()
        jobs_data = json.loads(data['cron_jobs'])
        self.assertEqual(len(jobs_data), 3)

        # Verify all jobs have required fields
        for job in jobs_data:
            self.assertIn('func_name', job)
            self.assertIn('queue_name', job)
            self.assertTrue('interval' in job or 'cron' in job)

    def test_cron_scheduler_save_and_restore(self):
        """Test that save() and fetch() round-trip scheduler data and jobs correctly"""
        # Test with no jobs
        scheduler = CronScheduler(connection=self.connection, name='empty-scheduler')
        scheduler.config_file = 'test_config.py'
        scheduler.save()

        fetched_scheduler = CronScheduler.fetch('empty-scheduler', self.connection)
        self.assertEqual(fetched_scheduler.hostname, scheduler.hostname)
        self.assertEqual(fetched_scheduler.pid, scheduler.pid)
        self.assertEqual(fetched_scheduler.name, scheduler.name)
        self.assertEqual(fetched_scheduler.config_file, 'test_config.py')
        self.assertEqual(fetched_scheduler.created_at, scheduler.created_at)
        self.assertEqual(len(fetched_scheduler.get_jobs()), 0)

        # Test with jobs (including args and kwargs)
        scheduler = CronScheduler(self.connection, name='test-scheduler')
        scheduler.config_file = 'other_config.py'
        scheduler.register(say_hello, 'default', args=('hello',), kwargs={'name': 'world'}, interval=60, job_timeout=30)
        scheduler.register(do_nothing, 'high', cron='0 * * * *')
        scheduler.save()

        # Fetch and verify scheduler attributes
        fetched_scheduler = CronScheduler.fetch('test-scheduler', self.connection)
        self.assertEqual(fetched_scheduler.name, scheduler.name)

        # Verify jobs
        job_1, job_2 = fetched_scheduler.get_jobs()

        self.assertEqual(job_1.func_name, 'tests.fixtures.say_hello')
        self.assertEqual(job_1.queue_name, 'default')
        self.assertEqual(job_1.interval, 60)
        self.assertIsNone(job_1.cron)
        # Verify args and kwargs are preserved
        self.assertEqual(job_1.args, ('hello',))
        self.assertEqual(job_1.kwargs, {'name': 'world'})

        self.assertEqual(job_2.func_name, 'tests.fixtures.do_nothing')
        self.assertEqual(job_2.queue_name, 'high')
        self.assertEqual(job_2.cron, '0 * * * *')
        self.assertIsNone(job_2.interval)

        # Verify fetched jobs are monitoring-only
        self.assertIsNone(job_1.func)
        self.assertIsNone(job_2.func)
        with self.assertRaises(ValueError):
            job_1.enqueue(self.connection)

    def test_save_jobs_data_updates_timing(self):
        """Test that save_jobs_data() updates job timing information in Redis"""
        scheduler = CronScheduler(connection=self.connection, name='test-scheduler')

        # Register a job with interval
        job = scheduler.register(say_hello, 'default', interval=60)

        # Initial save
        scheduler.save()

        # Fetch initial state
        fetched_scheduler = CronScheduler.fetch('test-scheduler', self.connection)
        initial_jobs = fetched_scheduler.get_jobs()
        self.assertEqual(len(initial_jobs), 1)
        # New job hasn't run yet, so timing should be None
        self.assertIsNone(initial_jobs[0].latest_enqueue_time)

        # Simulate job execution by updating timing
        from datetime import datetime, timedelta, timezone

        last_enqueue_time = datetime.now(timezone.utc)
        next_time = last_enqueue_time + timedelta(seconds=60)
        job.latest_enqueue_time = last_enqueue_time
        job.next_enqueue_time = next_time

        # Save only jobs data (not full scheduler state)
        scheduler.save_jobs_data()

        # Fetch again and verify timing was updated
        fetched_scheduler = CronScheduler.fetch('test-scheduler', self.connection)
        updated_jobs = fetched_scheduler.get_jobs()
        self.assertEqual(len(updated_jobs), 1)

        # Verify timing information was persisted
        self.assertIsNotNone(updated_jobs[0].latest_enqueue_time)
        self.assertIsNotNone(updated_jobs[0].next_enqueue_time)
        self.assertEqual(
            updated_jobs[0].latest_enqueue_time.replace(microsecond=0), last_enqueue_time.replace(microsecond=0)
        )
        self.assertEqual(updated_jobs[0].next_enqueue_time.replace(microsecond=0), next_time.replace(microsecond=0))

    def test_cron_scheduler_restore_edge_cases(self):
        """Test that restore() handles missing and malformed cron_jobs data gracefully"""
        # Test missing cron_jobs field (backwards compatibility)
        data = {
            b'hostname': b'test-host',
            b'pid': b'12345',
            b'name': b'old-scheduler',
            b'created_at': b'2025-11-29T00:00:00.000000Z',
            b'config_file': b'config.py',
        }

        scheduler = CronScheduler(connection=self.connection, name='restored')
        scheduler.restore(data)
        self.assertEqual(len(scheduler.get_jobs()), 0)

        # Test malformed JSON in cron_jobs
        data[b'cron_jobs'] = b'{invalid json]'

        scheduler.restore(data)
        self.assertEqual(len(scheduler.get_jobs()), 0)

    def test_cron_scheduler_fetch_from_redis(self):
        """Test that CronScheduler can be fetched from Redis"""
        # Test fetch after save()
        scheduler = CronScheduler(connection=self.connection, name='persistent-scheduler')
        scheduler.config_file = 'persistent_config.py'
        scheduler.save()

        fetched_scheduler = CronScheduler.fetch('persistent-scheduler', self.connection)
        self.assertEqual(fetched_scheduler.name, scheduler.name)

        # Test fetch after register_birth()
        scheduler = CronScheduler(connection=self.connection)
        scheduler.register_birth()

        fetched_scheduler = CronScheduler.fetch(scheduler.name, self.connection)
        self.assertEqual(fetched_scheduler.name, scheduler.name)
        self.assertEqual(fetched_scheduler.created_at, scheduler.created_at)

        # Test that fetching a nonexistent scheduler raises SchedulerNotFound
        with self.assertRaises(SchedulerNotFound):
            CronScheduler.fetch('nonexistent-scheduler', self.connection)

    def test_cron_scheduler_default_name(self):
        """Test that CronScheduler creates a default name if none provided"""
        scheduler = CronScheduler(connection=self.connection)
        # Name should follow pattern: hostname:pid:random_suffix
        expected_prefix = f'{scheduler.hostname}:{scheduler.pid}:'
        self.assertTrue(scheduler.name.startswith(expected_prefix))
        # Random suffix should be 6 characters (hex)
        suffix = scheduler.name[len(expected_prefix) :]
        self.assertEqual(len(suffix), 6)
        self.assertTrue(all(c in '0123456789abcdef' for c in suffix))

    def test_register_birth_and_death(self):
        """Test that register_birth and register_death manage scheduler registry and Redis hash"""
        scheduler = CronScheduler(connection=self.connection)

        # Register birth
        scheduler.register_birth()

        # Verify scheduler is in registry
        registered_keys = get_keys(self.connection)
        self.assertIn(scheduler.name, registered_keys)

        # Verify Redis hash data was saved
        self.assertTrue(self.connection.exists(scheduler.key))

        # Verify TTL is set (should be 60 seconds)
        ttl = self.connection.ttl(scheduler.key)
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 60)

        # Register death
        scheduler.register_death()

        # Verify scheduler is no longer in registry
        registered_keys = get_keys(self.connection)
        self.assertNotIn(scheduler.name, registered_keys)

    def test_heartbeat(self):
        """Test that heartbeat() updates scheduler's timestamp in registry and extends TTL"""
        scheduler = CronScheduler(connection=self.connection)

        # Ensure registry is clean
        registry_key = get_registry_key()
        self.connection.delete(registry_key)

        # Register scheduler first (heartbeat only works on registered schedulers)
        scheduler.register_birth()
        initial_score = self.connection.zscore(registry_key, scheduler.name)
        self.assertIsNotNone(initial_score)

        # Verify initial TTL (should be 60 seconds from register_birth)
        initial_ttl = self.connection.ttl(scheduler.key)
        self.assertTrue(0 < initial_ttl <= 60)

        # Wait a brief moment to ensure timestamp difference
        time.sleep(0.01)

        scheduler.heartbeat()
        new_score = self.connection.zscore(registry_key, scheduler.name)
        self.assertIsNotNone(new_score)
        self.assertGreater(cast(float, new_score), cast(float, initial_score))

        # Verify TTL was extended to 120 seconds
        new_ttl = self.connection.ttl(scheduler.key)
        self.assertTrue(60 < new_ttl <= 120)

        scheduler.register_death()

        # Test heartbeat on unregistered scheduler
        unregistered_scheduler = CronScheduler(connection=self.connection, name='unregistered-scheduler')

        # This should not raise an exception, but should log a warning
        unregistered_scheduler.heartbeat()

        # Verify unregistered scheduler is still not in registry
        score = self.connection.zscore(registry_key, 'unregistered-scheduler')
        self.assertIsNone(score)

    @patch('rq.cron.CronScheduler.register_death')
    @patch('rq.cron.CronScheduler._install_signal_handlers')
    def test_start_always_calls_register_death_on_exception(self, mock_signal_handlers, mock_register_death):
        """Test that start() calls register_death even when an exception occurs in the main loop"""
        scheduler = CronScheduler(connection=self.connection, name='test-scheduler')

        # Mock enqueue_jobs to raise an exception
        with patch.object(scheduler, 'enqueue_jobs', side_effect=Exception('Test exception')):
            with self.assertRaises(Exception) as cm:
                scheduler.start()

            self.assertEqual(str(cm.exception), 'Test exception')

            # Verify register_death was still called
            mock_signal_handlers.assert_called_once()
            mock_register_death.assert_called_once()

    @patch('rq.cron.CronScheduler.register_death')
    def test_start_handles_keyboard_interrupt(self, mock_register_death):
        """Test that start() handles KeyboardInterrupt gracefully"""
        scheduler = CronScheduler(connection=self.connection, name='test-scheduler')

        # Mock enqueue_jobs to raise KeyboardInterrupt
        with patch.object(scheduler, 'enqueue_jobs', side_effect=KeyboardInterrupt('Ctrl+C')):
            # start() should handle KeyboardInterrupt without re-raising it
            scheduler.start()

            # Verify lifecycle methods were called
            mock_register_death.assert_called_once()

    def test_sigint_handling(self):
        """Test that sending SIGINT to the process stops the scheduler"""
        conn_kwargs = self.connection.connection_pool.connection_kwargs
        scheduler_process = Process(target=run_scheduler, args=(conn_kwargs,))
        scheduler_process.start()
        assert scheduler_process.pid
        time.sleep(0.2)
        # Ensure scheduler is registered (name will have random suffix)
        scheduler_prefix = f'{socket.gethostname()}:{scheduler_process.pid}:'

        # Find scheduler with matching prefix
        matching_scheduler = None
        for key in get_keys(self.connection):
            if key.startswith(scheduler_prefix):
                matching_scheduler = key
                break

        self.assertTrue(matching_scheduler)

        os.kill(scheduler_process.pid, signal.SIGINT)

        scheduler_process.join(timeout=2)
        self.assertFalse(scheduler_process.is_alive())

        # Verify scheduler is no longer registered
        keys = get_keys(self.connection)
        self.assertEqual([key for key in keys if key.startswith(scheduler_prefix)], [])

    def test_last_heartbeat_property(self):
        """Test that last_heartbeat property works correctly in all scenarios"""
        scheduler = CronScheduler(connection=self.connection)

        # Ensure registry is clean
        registry_key = get_registry_key()
        self.connection.delete(registry_key)

        # Register scheduler and verify heartbeat timestamp
        before_registration = datetime.now(timezone.utc)
        scheduler.register_birth()
        initial_heartbeat = scheduler.last_heartbeat

        assert initial_heartbeat

        after_registration = datetime.now(timezone.utc)
        # Heartbeat should be between before and after registration
        self.assertGreaterEqual(initial_heartbeat, before_registration - timedelta(seconds=1))
        self.assertLessEqual(initial_heartbeat, after_registration + timedelta(seconds=1))

        # Wait and send heartbeat, should update timestamp
        time.sleep(0.01)
        scheduler.heartbeat()

        new_heartbeat = scheduler.last_heartbeat
        assert new_heartbeat
        self.assertGreater(new_heartbeat, initial_heartbeat)

        # After death, should return None
        scheduler.register_death()
        self.assertIsNone(scheduler.last_heartbeat)

    def test_all(self):
        """Test that CronScheduler.all() returns all registered schedulers"""
        # Clean up any existing schedulers
        registry_key = get_registry_key()
        self.connection.delete(registry_key)

        # Create multiple schedulers with default names (now unique due to random suffix)
        scheduler1 = CronScheduler(connection=self.connection)
        scheduler2 = CronScheduler(connection=self.connection)
        scheduler3 = CronScheduler(connection=self.connection)

        # Register births
        scheduler1.register_birth()
        scheduler2.register_birth()
        scheduler3.register_birth()

        # Test all() method
        all_schedulers = CronScheduler.all(self.connection)

        # Should return all 3 schedulers
        self.assertEqual(set(all_schedulers), {scheduler1, scheduler2, scheduler3})

        # Test with cleanup disabled
        all_schedulers_no_cleanup = CronScheduler.all(self.connection, cleanup=False)
        self.assertEqual(len(all_schedulers_no_cleanup), 3)

        # Register death for cleanup
        scheduler1.register_death()
        scheduler2.register_death()
        scheduler3.register_death()

    def test_equality(self):
        """Test that CronScheduler equality works correctly"""
        # Schedulers with same name should be equal
        scheduler1 = CronScheduler(connection=self.connection, name='test-scheduler')
        scheduler2 = CronScheduler(connection=self.connection, name='test-scheduler')

        self.assertEqual(scheduler1, scheduler2)
        self.assertEqual(hash(scheduler1), hash(scheduler2))

        # Schedulers with different names should not be equal
        scheduler3 = CronScheduler(connection=self.connection, name='different-scheduler')
        self.assertNotEqual(scheduler1, scheduler3)
        self.assertNotEqual(hash(scheduler1), hash(scheduler3))

        # Scheduler should not equal non-scheduler objects
        self.assertNotEqual(scheduler1, 'not-a-scheduler')
