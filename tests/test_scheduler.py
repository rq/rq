from datetime import datetime, timedelta

from rq import Queue
from rq.job import Job
from rq.registry import ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.utils import current_timestamp

from .fixtures import say_hello
from tests import RQTestCase


class TestScheduledJobRegistry(RQTestCase):

    def test_init(self):
        """Scheduler can be instantiated with queues or queue names"""
        foo_queue = Queue('foo', connection=self.testconn)
        # bar_queue = Queue('bar', connection=self.testconn)
        scheduler = RQScheduler([foo_queue, 'bar'], connection=self.testconn)
        self.assertEqual(scheduler._queue_names, ['foo', 'bar'])

    def test_get_jobs_to_enqueue(self):
        """Getting job ids to enqueue from ScheduledJobRegistry."""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        timestamp = current_timestamp()

        self.testconn.zadd(registry.key, {'foo': 1})
        self.testconn.zadd(registry.key, {'bar': timestamp + 10})
        self.testconn.zadd(registry.key, {'baz': timestamp + 30})

        self.assertEqual(registry.get_jobs_to_enqueue(), ['foo'])
        self.assertEqual(registry.get_jobs_to_enqueue(timestamp + 20),
                         ['foo', 'bar'])

    def test_lock_acquisition(self):
        """Test lock acquisition"""
        name = 'lock-test'
        self.assertTrue(RQScheduler.acquire_lock(name, self.testconn))
        self.assertFalse(RQScheduler.acquire_lock(name, self.testconn))

        # If key is manually deleted, lock acquisition should be successful again
        self.testconn.delete(RQScheduler.get_locking_key(name))
        self.assertTrue(RQScheduler.acquire_lock(name, self.testconn))

    def test_enqueue_scheduled_jobs(self):
        """Scheduler can enqueue scheduled jobs"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        registry.schedule(job, datetime(2019, 1, 1))
        scheduler = RQScheduler([queue], connection=self.testconn)
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

        # After job is scheduled, registry should be empty
        self.assertEqual(len(registry), 0)

        # Jobs scheduled in the far future should not be affected
        registry.schedule(job, datetime(2100, 1, 1))
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

    def test_get_scheduled_time(self):
        """get_scheduled_time() returns job's scheduled datetime"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)

        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        dt = datetime(2019, 1, 1)
        registry.schedule(job, datetime(2019, 1, 1))
        self.assertEqual(registry.get_scheduled_time(job), dt)
        # get_scheduled_time() should also work with job ID
        self.assertEqual(registry.get_scheduled_time(job.id), dt)

    def test_schedule(self):
        """Adding job with the correct score to ScheduledJobRegistry"""
        queue = Queue(connection=self.testconn)
        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        registry = ScheduledJobRegistry(queue=queue)
        registry.schedule(job, datetime(2019, 1, 1))
        self.assertEqual(self.testconn.zscore(registry.key, job.id),
                         1546300800)  # 2019-01-01 UTC in Unix timestamp

        # Only run this test if `timezone` is available (Python 3.2+)
        try:
            from datetime import timezone
            # Score is always stored in UTC even if datetime is in a different tz
            tz = timezone(timedelta(hours=7))
            job = Job.create('myfunc', connection=self.testconn)
            job.save()
            registry.schedule(job, datetime(2019, 1, 1, 7, tzinfo=tz))
            self.assertEqual(self.testconn.zscore(registry.key, job.id),
                             1546300800)  # 2019-01-01 UTC in Unix timestamp
        except ImportError:
            pass


class TestQueue(RQTestCase):

    def test_enqueue_at(self):
        """queue.enqueue_at() puts job in the scheduled"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.testconn)

        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        queue.enqueue_at(datetime(2019, 1, 1), say_hello)
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 1)

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(registry), 0)
