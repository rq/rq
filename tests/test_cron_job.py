from datetime import datetime, timedelta

from rq import Queue, utils
from rq.cron import CronJob
from tests import RQTestCase
from tests.fixtures import say_hello


class TestCronJob(RQTestCase):
    """Tests for the CronJob class"""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)

    def test_cron_job_initialization(self):
        """CronJob correctly initializes with the provided parameters"""
        # Test with minimum required parameters (interval)
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=60)
        self.assertEqual(cron_job.func, say_hello)
        self.assertEqual(cron_job.args, ())
        self.assertEqual(cron_job.kwargs, {})
        self.assertEqual(cron_job.interval, 60)
        self.assertIsNone(cron_job.cron)
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
        # Test with cron job (returns far future for no latest_run_time initially)
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        # Without latest_run_time set, get_next_run_time uses current time
        next_run = cron_job.get_next_run_time()
        self.assertIsInstance(next_run, datetime)

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
        # Test with cron job
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        test_time = utils.now()
        cron_job.set_run_time(test_time)

        # Check latest_run_time is set correctly
        self.assertEqual(cron_job.latest_run_time, test_time)

        # Since cron is set, next_run_time should be calculated
        self.assertIsNotNone(cron_job.next_run_time)

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

    def test_cron_job_initialization_with_cron_string(self):
        """CronJob correctly initializes with cron string parameters"""
        cron_expr = '0 9 * * 1-5'  # 9 AM weekdays
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        self.assertEqual(cron_job.func, say_hello)
        self.assertEqual(cron_job.cron, cron_expr)
        self.assertIsNone(cron_job.interval)
        self.assertEqual(cron_job.queue_name, self.queue.name)

        # next_run_time should be set immediately upon initialization
        self.assertIsNotNone(cron_job.next_run_time)

        # latest_run_time should still be None (hasn't run yet)
        self.assertIsNone(cron_job.latest_run_time)

        # For comparison, interval jobs don't set next_run_time during initialization
        interval_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=60)
        self.assertIsNone(interval_job.next_run_time)  # Not set until first run

    def test_get_next_run_time_with_cron_string(self):
        """Test that get_next_run_time correctly calculates next run time using cron expression"""
        # Test daily at 9 AM
        cron_expr = '0 9 * * *'
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        # Set a base time to 8 AM today
        base_time = datetime(2023, 10, 27, 8, 0, 0)
        cron_job.set_run_time(base_time)

        next_run_time = cron_job.get_next_run_time()
        expected_next_run = datetime(2023, 10, 27, 9, 0, 0)  # 9 AM same day
        self.assertEqual(next_run_time, expected_next_run)

        # If we're already past 9 AM, should schedule for next day
        base_time = datetime(2023, 10, 27, 10, 0, 0)
        cron_job.set_run_time(base_time)

        next_run_time = cron_job.get_next_run_time()
        expected_next_run = datetime(2023, 10, 28, 9, 0, 0)  # 9 AM next day
        self.assertEqual(next_run_time, expected_next_run)

    def test_should_run_with_cron_string(self):
        """Test should_run method logic with cron expressions"""
        # Job with cron that has not run yet should NOT run immediately (crontab behavior)
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        # next_run_time should already be set during initialization
        self.assertIsNotNone(cron_job.next_run_time)

        # Job with future next_run_time should not run yet
        cron_job.next_run_time = utils.now() + timedelta(hours=1)
        self.assertFalse(cron_job.should_run())

        # Job with past next_run_time should run
        cron_job.next_run_time = utils.now() - timedelta(minutes=5)
        self.assertTrue(cron_job.should_run())

    def test_cron_weekday_expressions(self):
        """Test cron expressions with specific weekday patterns"""
        # Monday to Friday at 9 AM
        cron_expr = '0 9 * * 1-5'
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        # Set to Friday 9 AM
        friday_time = datetime(2023, 10, 27, 9, 0, 0)  # Assuming this is a Friday
        cron_job.set_run_time(friday_time)

        next_run_time = cron_job.get_next_run_time()

        # Next run should be Monday (skip weekend)
        # We can verify it's not Saturday or Sunday by checking the weekday
        self.assertIn(next_run_time.weekday(), [0, 1, 2, 3, 4])  # Monday=0, Friday=4
        self.assertEqual(next_run_time.hour, 9)
        self.assertEqual(next_run_time.minute, 0)

    def test_cron_job_serialization_roundtrip(self):
        """Test CronJob serialization and deserialization with both interval and cron jobs"""
        # Test with interval job
        interval_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=('test', 'args'),  # These won't be serialized
            kwargs={'test': 'kwarg'},  # These won't be serialized
            interval=60,
            timeout=30,
            result_ttl=600,
            meta={'test': 'meta'},
        )

        # Serialize and deserialize
        interval_data = interval_job.to_dict()
        restored_interval = CronJob.from_dict(interval_data)

        # Check serialized data structure
        self.assertEqual(interval_data['func_name'], 'tests.fixtures.say_hello')
        self.assertEqual(interval_data['queue_name'], self.queue.name)
        self.assertEqual(interval_data['interval'], 60)
        self.assertIsNone(interval_data['cron'])
        self.assertEqual(interval_data['timeout'], 30)
        self.assertEqual(interval_data['result_ttl'], 600)
        self.assertEqual(interval_data['meta'], {'test': 'meta'})

        # Check restored fields match original
        self.assertEqual(restored_interval.queue_name, interval_job.queue_name)
        self.assertEqual(restored_interval.interval, interval_job.interval)
        self.assertEqual(restored_interval.cron, interval_job.cron)
        self.assertEqual(restored_interval.job_options, interval_job.job_options)
        self.assertIsNone(restored_interval.func)

        # Test with cron job
        cron_job = CronJob(
            func=say_hello, queue_name='priority_queue', cron='0 9 * * MON-FRI', ttl=300, failure_ttl=1800
        )

        # Serialize and deserialize
        cron_data = cron_job.to_dict()
        restored_cron = CronJob.from_dict(cron_data)

        # Check serialized data structure
        self.assertEqual(cron_data['func_name'], 'tests.fixtures.say_hello')
        self.assertEqual(cron_data['queue_name'], 'priority_queue')
        self.assertIsNone(cron_data['interval'])
        self.assertEqual(cron_data['cron'], '0 9 * * MON-FRI')
        self.assertEqual(cron_data['ttl'], 300)
        self.assertEqual(cron_data['failure_ttl'], 1800)
        # result_ttl should have default value
        self.assertEqual(cron_data['result_ttl'], 500)

        # Check restored fields match original
        self.assertEqual(restored_cron.queue_name, cron_job.queue_name)
        self.assertEqual(restored_cron.interval, cron_job.interval)
        self.assertEqual(restored_cron.cron, cron_job.cron)
        self.assertEqual(restored_cron.job_options, cron_job.job_options)
        self.assertIsNone(restored_cron.func)
