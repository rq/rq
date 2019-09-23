import os
import time

from datetime import datetime, timedelta
from multiprocessing import Process

from rq import Queue
from rq.compat import utc
from rq.exceptions import NoSuchJobError
from rq.job import Job
from rq.registry import FinishedJobRegistry, ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.utils import current_timestamp
from rq.worker import Worker

from .fixtures import kill_worker, say_hello
from tests import RQTestCase

import mock


class TestScheduledJobRegistry(RQTestCase):

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

    def test_get_scheduled_time(self):
        """get_scheduled_time() returns job's scheduled datetime"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)

        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        dt = datetime(2019, 1, 1, tzinfo=utc)
        registry.schedule(job, datetime(2019, 1, 1, tzinfo=utc))
        self.assertEqual(registry.get_scheduled_time(job), dt)
        # get_scheduled_time() should also work with job ID
        self.assertEqual(registry.get_scheduled_time(job.id), dt)

        # registry.get_scheduled_time() raises NoSuchJobError if
        # job.id is not found
        self.assertRaises(NoSuchJobError, registry.get_scheduled_time, '123')
        

    def test_schedule(self):
        """Adding job with the correct score to ScheduledJobRegistry"""
        queue = Queue(connection=self.testconn)
        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        registry = ScheduledJobRegistry(queue=queue)

        # If we pass in a datetime with no timezone, `schedule()`
        # assumes local timezone so depending on your local timezone,
        # the timestamp maybe different
        registry.schedule(job, datetime(2019, 1, 1))
        self.assertEqual(self.testconn.zscore(registry.key, job.id),
                         1546300800 + time.timezone)  # 2019-01-01 UTC in Unix timestamp

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


class TestScheduler(RQTestCase):

    def test_init(self):
        """Scheduler can be instantiated with queues or queue names"""
        foo_queue = Queue('foo', connection=self.testconn)
        # bar_queue = Queue('bar', connection=self.testconn)
        scheduler = RQScheduler([foo_queue, 'bar'], connection=self.testconn)
        self.assertEqual(scheduler._queue_names, {'foo', 'bar'})
        self.assertEqual(scheduler.status, RQScheduler.Status.STOPPED)

    def test_lock_acquisition(self):
        """Test lock acquisition"""
        name_1 = 'lock-test-1'
        name_2 = 'lock-test-2'
        name_3 = 'lock-test-3'
        scheduler = RQScheduler([name_1], self.testconn)

        self.assertEqual(scheduler.acquire_locks(), {name_1})
        self.assertEqual(scheduler._acquired_locks, {name_1})
        self.assertEqual(scheduler.acquire_locks(), set([]))

        # Only name_2 is returned since name_1 is already locked
        scheduler = RQScheduler([name_1, name_2], self.testconn)
        self.assertEqual(scheduler.acquire_locks(), {name_2})
        self.assertEqual(scheduler._acquired_locks, {name_2})

        # When a new lock is successfully acquired, _acquired_locks is added
        scheduler._queue_names.add(name_3)
        self.assertEqual(scheduler.acquire_locks(), {name_3})
        self.assertEqual(scheduler._acquired_locks, {name_2, name_3})
    
    def test_lock_acquisition_with_auto_start(self):
        """Test lock acquisition with auto_start=True"""
        scheduler = RQScheduler(['auto-start'], self.testconn)
        with mock.patch.object(scheduler, 'start') as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 1)

        # If process has started, scheduler.start() won't be called
        scheduler = RQScheduler(['auto-start2'], self.testconn)
        scheduler._process = 1
        with mock.patch.object(scheduler, 'start') as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 0)
    
    def test_heartbeat(self):
        """Test that heartbeat updates locking keys TTL"""
        name_1 = 'lock-test-1'
        name_2 = 'lock-test-2'
        scheduler = RQScheduler([name_1, name_2], self.testconn)
        scheduler.acquire_locks()

        locking_key_1 = RQScheduler.get_locking_key(name_1)
        locking_key_2 = RQScheduler.get_locking_key(name_2)

        with self.testconn.pipeline() as pipeline:
            pipeline.expire(locking_key_1, 1000)
            pipeline.expire(locking_key_2, 1000)

        scheduler.heartbeat()
        self.assertEqual(self.testconn.ttl(locking_key_1), 6)
        self.assertEqual(self.testconn.ttl(locking_key_1), 6)

        # scheduler.stop() releases locks and sets status to STOPPED
        scheduler._status = scheduler.Status.WORKING
        scheduler.stop()
        self.assertFalse(self.testconn.exists(locking_key_1))
        self.assertFalse(self.testconn.exists(locking_key_2))
        self.assertEqual(scheduler.status, scheduler.Status.STOPPED)

        # Heartbeat also works properly for schedulers with a single queue
        scheduler = RQScheduler([name_1], self.testconn)
        scheduler.acquire_locks()
        self.testconn.expire(locking_key_1, 1000)
        scheduler.heartbeat()
        self.assertEqual(self.testconn.ttl(locking_key_1), 6)

    def test_enqueue_scheduled_jobs(self):
        """Scheduler can enqueue scheduled jobs"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        job = Job.create('myfunc', connection=self.testconn)
        job.save()
        registry.schedule(job, datetime(2019, 1, 1))
        scheduler = RQScheduler([queue], connection=self.testconn)
        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

        # After job is scheduled, registry should be empty
        self.assertEqual(len(registry), 0)

        # Jobs scheduled in the far future should not be affected
        registry.schedule(job, datetime(2100, 1, 1))
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)
    
    def test_prepare_registries(self):
        """prepare_registries() creates self._scheduled_job_registries"""
        foo_queue = Queue('foo', connection=self.testconn)
        bar_queue = Queue('bar', connection=self.testconn)
        scheduler = RQScheduler([foo_queue, bar_queue], connection=self.testconn)
        self.assertEqual(scheduler._scheduled_job_registries, [])
        scheduler.prepare_registries([foo_queue.name])
        self.assertEqual(scheduler._scheduled_job_registries, [ScheduledJobRegistry(queue=foo_queue)])
        scheduler.prepare_registries([foo_queue.name, bar_queue.name])
        self.assertEqual(
            scheduler._scheduled_job_registries,
            [ScheduledJobRegistry(queue=foo_queue), ScheduledJobRegistry(queue=bar_queue)]
        )


class TestWorker(RQTestCase):

    def test_work_burst(self):
        """worker.work() with scheduler enabled works properly"""
        queue = Queue(connection=self.testconn)
        worker = Worker(queues=[queue], connection=self.testconn)
        worker.work(burst=True, with_scheduler=True)

    def test_work(self):
        queue = Queue(connection=self.testconn)
        worker = Worker(queues=[queue], connection=self.testconn)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1), say_hello)
        worker.work(burst=False, with_scheduler=True)        
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)


class TestQueue(RQTestCase):

    def test_enqueue_at(self):
        """queue.enqueue_at() puts job in the scheduled"""
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.testconn)
        scheduler.acquire_locks()

        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        queue.enqueue_at(datetime(2019, 1, 1), say_hello)
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 1)

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(registry), 0)
