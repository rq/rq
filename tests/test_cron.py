
from datetime import datetime, timedelta

from rq import Queue
from rq.job import Job
from rq.cron import Cron, CronJob
from tests import RQTestCase
from tests.fixtures import say_hello, echo


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

        now = datetime.now()
        next_run = cron_job.get_next_run_time()

        # Check that next_run is about interval seconds from now
        # Allow 2 seconds tolerance for test execution time
        self.assertTrue(
            now + timedelta(seconds=interval - 5) <= next_run <= now + timedelta(seconds=interval + 5),
            f"Next run time {next_run} not within expected range"
        )

    def test_should_run(self):
        """Test should_run method logic"""
        # Job with no interval should not run automatically
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=None)
        self.assertFalse(cron_job.should_run())

        # Job with future next_run should not run yet
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=3600)  # 1 hour
        cron_job.next_run = datetime.now() + timedelta(minutes=5)
        self.assertFalse(cron_job.should_run())

        # Job with past next_run should run
        cron_job = CronJob(func=say_hello, queue_name=self.queue.name, interval=60)
        cron_job.next_run = datetime.now() - timedelta(seconds=5)
        self.assertTrue(cron_job.should_run())

    def test_enqueue(self):
        """Test that enqueue correctly creates a job in the queue"""
        cron_job = CronJob(
            func=echo,
            queue_name=self.queue.name,
            args=("Hello",),
            interval=60,
        )

        # Capture the original next_run
        original_next_run = cron_job.next_run

        # Enqueue the job
        job = cron_job.enqueue(self.connection)

        # Check job was created correctly
        self.assertIsInstance(job, Job)

        # Fetch job from queue and verify
        jobs = self.queue.get_jobs()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].id, job.id)

        # Verify next_run was updated
        self.assertGreater(cron_job.next_run, original_next_run)


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
            func=echo,
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
