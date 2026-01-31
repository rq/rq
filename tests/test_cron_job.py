from datetime import datetime, timedelta, timezone

from rq import Queue, utils
from rq.cron import CronJob, CronScheduler
from rq.utils import NOT_JSON_SERIALIZABLE
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
            job_timeout=timeout,
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
        self.assertEqual(cron_job.job_options['job_timeout'], timeout)
        self.assertEqual(cron_job.job_options['result_ttl'], result_ttl)
        self.assertEqual(cron_job.job_options['ttl'], ttl)
        self.assertEqual(cron_job.job_options['failure_ttl'], failure_ttl)
        self.assertEqual(cron_job.job_options['meta'], meta)

    def test_get_next_enqueue_time(self):
        """Test that get_next_enqueue_time correctly calculates the next run time"""
        # Test with cron job (returns far future for no latest_enqueue_time initially)
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        # Without latest_enqueue_time set, get_next_enqueue_time uses current time
        next_run = cron_job.get_next_enqueue_time()
        self.assertIsInstance(next_run, datetime)

        # Test with a specific interval
        interval = 60  # 60 seconds
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=interval)

        now = utils.now()
        cron_job.set_enqueue_time(now)
        next_enqueue_time = cron_job.get_next_enqueue_time()

        # Check that next_enqueue_time is about interval seconds from now
        # Allow 2 seconds tolerance for test execution time
        self.assertTrue(
            now + timedelta(seconds=interval - 5) <= next_enqueue_time <= now + timedelta(seconds=interval + 5),
            f'Next run time {next_enqueue_time} not within expected range',
        )

    def test_should_run(self):
        """Test should_run method logic"""

        # Job with interval that has not run yet always returns True
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=3600)
        self.assertTrue(cron_job.should_run())

        # Job with future next_enqueue_time should not run yet
        cron_job.latest_enqueue_time = utils.now() - timedelta(seconds=5)
        cron_job.next_enqueue_time = utils.now() + timedelta(minutes=5)
        self.assertFalse(cron_job.should_run())

        # Job with past next_enqueue_time should run
        cron_job.next_enqueue_time = utils.now() - timedelta(seconds=5)
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

    def test_enqueue_with_job_timeout(self):
        """Test that job_timeout is properly set on the job and not passed to the function"""
        timeout_value = 180
        cron_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=('World',),
            interval=60,
            job_timeout=timeout_value,
        )
        job = cron_job.enqueue(self.connection)
        self.assertEqual(job.timeout, timeout_value)
        # Verify job can be executed without TypeError
        job.perform()

    def test_set_enqueue_time(self):
        """Test that set_enqueue_time correctly sets latest run time and updates next run time"""
        # Test with cron job
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        test_time = utils.now()
        cron_job.set_enqueue_time(test_time)

        # Check latest_enqueue_time is set correctly
        self.assertEqual(cron_job.latest_enqueue_time, test_time)

        # Since cron is set, next_enqueue_time should be calculated
        self.assertIsNotNone(cron_job.next_enqueue_time)

        # Test with an interval
        interval = 60  # 60 seconds
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=interval)
        cron_job.set_enqueue_time(test_time)

        # Check latest_enqueue_time is set correctly
        self.assertEqual(cron_job.latest_enqueue_time, test_time)

        # Check that next_enqueue_time is calculated correctly
        next_enqueue_time = test_time + timedelta(seconds=interval)
        self.assertEqual(cron_job.next_enqueue_time, next_enqueue_time)

        # Test updating the run time
        test_time = test_time + timedelta(seconds=30)
        cron_job.set_enqueue_time(test_time)

        # Check latest_enqueue_time is updated
        self.assertEqual(cron_job.latest_enqueue_time, test_time)

        # Check that next_enqueue_time is recalculated
        new_expected_next_run = test_time + timedelta(seconds=interval)
        self.assertEqual(cron_job.next_enqueue_time, new_expected_next_run)

    def test_cron_job_initialization_with_cron_string(self):
        """CronJob correctly initializes with cron string parameters"""
        cron_expr = '0 9 * * 1-5'  # 9 AM weekdays
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        self.assertEqual(cron_job.func, say_hello)
        self.assertEqual(cron_job.cron, cron_expr)
        self.assertIsNone(cron_job.interval)
        self.assertEqual(cron_job.queue_name, self.queue.name)

        # next_enqueue_time should be set immediately upon initialization
        self.assertIsNotNone(cron_job.next_enqueue_time)

        # latest_enqueue_time should still be None (hasn't run yet)
        self.assertIsNone(cron_job.latest_enqueue_time)

        # For comparison, interval jobs don't set next_enqueue_time during initialization
        interval_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=60)
        self.assertIsNone(interval_job.next_enqueue_time)  # Not set until first run

    def test_get_next_enqueue_time_with_cron_string(self):
        """Test that get_next_enqueue_time correctly calculates next run time using cron expression"""
        # Test daily at 9 AM
        cron_expr = '0 9 * * *'
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        # Set a base time to 8 AM today
        base_time = datetime(2023, 10, 27, 8, 0, 0)
        cron_job.set_enqueue_time(base_time)

        next_enqueue_time = cron_job.get_next_enqueue_time()
        expected_next_run = datetime(2023, 10, 27, 9, 0, 0)  # 9 AM same day
        self.assertEqual(next_enqueue_time, expected_next_run)

        # If we're already past 9 AM, should schedule for next day
        base_time = datetime(2023, 10, 27, 10, 0, 0)
        cron_job.set_enqueue_time(base_time)

        next_enqueue_time = cron_job.get_next_enqueue_time()
        expected_next_run = datetime(2023, 10, 28, 9, 0, 0)  # 9 AM next day
        self.assertEqual(next_enqueue_time, expected_next_run)

    def test_should_run_with_cron_string(self):
        """Test should_run method logic with cron expressions"""
        # Job with cron that has not run yet should NOT run immediately (crontab behavior)
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron='0 9 * * *')
        # next_enqueue_time should already be set during initialization
        self.assertIsNotNone(cron_job.next_enqueue_time)

        # Job with future next_enqueue_time should not run yet
        cron_job.next_enqueue_time = utils.now() + timedelta(hours=1)
        self.assertFalse(cron_job.should_run())

        # Job with past next_enqueue_time should run
        cron_job.next_enqueue_time = utils.now() - timedelta(minutes=5)
        self.assertTrue(cron_job.should_run())

    def test_cron_weekday_expressions(self):
        """Test cron expressions with specific weekday patterns"""
        # Monday to Friday at 9 AM
        cron_expr = '0 9 * * 1-5'
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, cron=cron_expr)

        # Set to Friday 9 AM
        friday_time = datetime(2023, 10, 27, 9, 0, 0)  # Assuming this is a Friday
        cron_job.set_enqueue_time(friday_time)

        next_enqueue_time = cron_job.get_next_enqueue_time()

        # Next run should be Monday (skip weekend)
        # We can verify it's not Saturday or Sunday by checking the weekday
        self.assertIn(next_enqueue_time.weekday(), [0, 1, 2, 3, 4])  # Monday=0, Friday=4
        self.assertEqual(next_enqueue_time.hour, 9)
        self.assertEqual(next_enqueue_time.minute, 0)

    def test_cron_job_serialization_roundtrip(self):
        """Test CronJob serialization and deserialization with both interval and cron jobs"""
        # Test with interval job
        interval_job = CronJob(
            func=say_hello,
            queue_name=self.queue.name,
            args=('test', 'args'),
            kwargs={'test': 'kwarg'},
            interval=60,
            job_timeout=30,
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
        self.assertEqual(interval_data['job_timeout'], 30)
        self.assertEqual(interval_data['result_ttl'], 600)
        self.assertEqual(interval_data['meta'], '{"test": "meta"}')  # JSON string

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

    def test_round_trip_serialization(self):
        """Test that round-trip serialization preserves timing, args, kwargs, and job_options"""
        original_job = CronJob(
            queue_name='default',
            func=say_hello,
            args=('hello', 123),
            kwargs={'name': 'world'},
            cron='0 9 * * *',
            job_timeout=300,
            result_ttl=1000,
            meta={'foo': 'bar'},
        )

        now_time = datetime.now(timezone.utc)
        original_job.latest_enqueue_time = now_time
        original_job.next_enqueue_time = now_time + timedelta(hours=1)

        restored_job = CronJob.from_dict(original_job.to_dict())

        # Assert timing preserved
        self.assertEqual(
            restored_job.latest_enqueue_time.replace(microsecond=0),
            original_job.latest_enqueue_time.replace(microsecond=0),
        )
        self.assertEqual(
            restored_job.next_enqueue_time.replace(microsecond=0), original_job.next_enqueue_time.replace(microsecond=0)
        )
        # Assert args and kwargs preserved
        self.assertEqual(restored_job.args, original_job.args)
        self.assertEqual(restored_job.kwargs, original_job.kwargs)
        # Assert job_options preserved
        self.assertEqual(restored_job.job_options, original_job.job_options)

    def test_non_serializable_arguments(self):
        """Test that from_dict gracefully handles the non-serializable placeholder through Redis"""

        scheduler = CronScheduler(connection=self.connection)
        scheduler.register(
            say_hello,
            'default',
            args=({1, 2, 3},),  # sets are not JSON serializable
            kwargs={'bad': {4, 5, 6}},
            interval=60,
        )
        scheduler.save()

        # Fetch from Redis - this should NOT raise
        fetched_scheduler = CronScheduler.fetch(scheduler.name, self.connection)
        job = fetched_scheduler.get_jobs()[0]

        # Non-serializable values should keep the placeholder for monitoring visibility
        self.assertEqual(job.args, NOT_JSON_SERIALIZABLE)
        self.assertEqual(job.kwargs, NOT_JSON_SERIALIZABLE)

    def test_to_dict_returns_json_strings_for_serializable_values(self):
        """Test that to_dict returns JSON strings for serializable args/kwargs"""
        job = CronJob(
            queue_name='default',
            func=say_hello,
            args=('hello', 123),
            kwargs={'name': 'world'},
            interval=60,
            meta={'foo': 'bar'},
        )

        job_dict = job.to_dict()
        # Now args/kwargs/meta are JSON strings, not raw Python objects
        self.assertEqual(job_dict['args'], '["hello", 123]')
        self.assertEqual(job_dict['kwargs'], '{"name": "world"}')
        self.assertEqual(job_dict['meta'], '{"foo": "bar"}')
