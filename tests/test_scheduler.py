import os
from datetime import datetime, timedelta, timezone
from multiprocessing import Process
from unittest import mock

import redis

from rq import Queue
from rq.defaults import DEFAULT_MAINTENANCE_TASK_INTERVAL
from rq.exceptions import NoSuchJobError
from rq.job import Job, Retry
from rq.registry import FinishedJobRegistry, ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.serializers import JSONSerializer
from rq.utils import current_timestamp
from rq.worker import Worker
from tests import RQTestCase, find_empty_redis_database, ssl_test

from .fixtures import kill_worker, say_hello


class CustomRedisConnection(redis.Connection):
    """Custom redis connection with a custom arg, used in test_custom_connection_pool"""

    def __init__(self, *args, custom_arg=None, **kwargs):
        self.custom_arg = custom_arg
        super().__init__(*args, **kwargs)

    def get_custom_arg(self):
        return self.custom_arg


class TestScheduledJobRegistry(RQTestCase):
    def test_get_jobs_to_enqueue(self):
        """Getting job ids to enqueue from ScheduledJobRegistry."""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        timestamp = current_timestamp()

        self.connection.zadd(registry.key, {'foo': 1})
        self.connection.zadd(registry.key, {'bar': timestamp + 10})
        self.connection.zadd(registry.key, {'baz': timestamp + 30})

        self.assertEqual(registry.get_jobs_to_enqueue(), ['foo'])
        self.assertEqual(registry.get_jobs_to_enqueue(timestamp + 20), ['foo', 'bar'])

    def test_get_jobs_to_schedule_with_chunk_size(self):
        """Max amount of jobs returns by get_jobs_to_schedule() equal to chunk_size"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        timestamp = current_timestamp()
        chunk_size = 5

        for index in range(0, chunk_size * 2):
            self.connection.zadd(registry.key, {'foo_{}'.format(index): 1})

        self.assertEqual(len(registry.get_jobs_to_schedule(timestamp, chunk_size)), chunk_size)
        self.assertEqual(len(registry.get_jobs_to_schedule(timestamp, chunk_size * 2)), chunk_size * 2)

    def test_get_scheduled_time(self):
        """get_scheduled_time() returns job's scheduled datetime"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)

        job = Job.create('myfunc', connection=self.connection)
        job.save()
        dt = datetime(2019, 1, 1, tzinfo=timezone.utc)
        registry.schedule(job, datetime(2019, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(registry.get_scheduled_time(job), dt)
        # get_scheduled_time() should also work with job ID
        self.assertEqual(registry.get_scheduled_time(job.id), dt)

        # registry.get_scheduled_time() raises NoSuchJobError if
        # job.id is not found
        self.assertRaises(NoSuchJobError, registry.get_scheduled_time, '123')

    def test_schedule(self):
        """Adding job with the correct score to ScheduledJobRegistry"""
        queue = Queue(connection=self.connection)
        job = Job.create('myfunc', connection=self.connection)
        job.save()
        registry = ScheduledJobRegistry(queue=queue)

        from datetime import timezone

        # If we pass in a datetime with no timezone, `schedule()`
        # assumes local timezone so depending on your local timezone,
        # the timestamp maybe different
        #
        # we need to account for the difference between a timezone
        # with DST active and without DST active.  The time.timezone
        # property isn't accurate when time.daylight is non-zero,
        # we'll test both.
        #
        # first, time.daylight == 0 (not in DST).
        # mock the sitatuoin for American/New_York not in DST (UTC - 5)
        # time.timezone = 18000
        # time.daylight = 0
        # time.altzone = 14400

        mock_day = mock.patch('time.daylight', 0)
        mock_tz = mock.patch('time.timezone', 18000)
        mock_atz = mock.patch('time.altzone', 14400)
        with mock_tz, mock_day, mock_atz:
            registry.schedule(job, datetime(2019, 1, 1))
            self.assertEqual(
                self.connection.zscore(registry.key, job.id), 1546300800 + 18000
            )  # 2019-01-01 UTC in Unix timestamp

            # second, time.daylight != 0 (in DST)
            # mock the sitatuoin for American/New_York not in DST (UTC - 4)
            # time.timezone = 18000
            # time.daylight = 1
            # time.altzone = 14400
            mock_day = mock.patch('time.daylight', 1)
            mock_tz = mock.patch('time.timezone', 18000)
            mock_atz = mock.patch('time.altzone', 14400)
            with mock_tz, mock_day, mock_atz:
                registry.schedule(job, datetime(2019, 1, 1))
                self.assertEqual(
                    self.connection.zscore(registry.key, job.id), 1546300800 + 14400
                )  # 2019-01-01 UTC in Unix timestamp

            # Score is always stored in UTC even if datetime is in a different tz
            tz = timezone(timedelta(hours=7))
            job = Job.create('myfunc', connection=self.connection)
            job.save()
            registry.schedule(job, datetime(2019, 1, 1, 7, tzinfo=tz))
            self.assertEqual(
                self.connection.zscore(registry.key, job.id), 1546300800
            )  # 2019-01-01 UTC in Unix timestamp


class TestScheduler(RQTestCase):
    def test_init(self):
        """Scheduler can be instantiated with queues or queue names"""
        foo_queue = Queue('foo', connection=self.connection)
        scheduler = RQScheduler([foo_queue, 'bar'], connection=self.connection)
        self.assertEqual(scheduler._queue_names, {'foo', 'bar'})
        self.assertEqual(scheduler.status, RQScheduler.Status.STOPPED)

    def test_should_reacquire_locks(self):
        """scheduler.should_reacquire_locks works properly"""
        queue = Queue(connection=self.connection)
        scheduler = RQScheduler([queue], connection=self.connection)
        self.assertTrue(scheduler.should_reacquire_locks)
        scheduler.acquire_locks()
        self.assertIsNotNone(scheduler.lock_acquisition_time)

        # scheduler.should_reacquire_locks always returns False if
        # scheduler.acquired_locks and scheduler._queue_names are the same
        self.assertFalse(scheduler.should_reacquire_locks)
        scheduler.lock_acquisition_time = datetime.now() - timedelta(seconds=DEFAULT_MAINTENANCE_TASK_INTERVAL + 6)
        self.assertFalse(scheduler.should_reacquire_locks)

        scheduler._queue_names = set(['default', 'foo'])
        self.assertTrue(scheduler.should_reacquire_locks)
        scheduler.acquire_locks()
        self.assertFalse(scheduler.should_reacquire_locks)

    def test_lock_acquisition(self):
        """Test lock acquisition"""
        name_1 = 'lock-test-1'
        name_2 = 'lock-test-2'
        name_3 = 'lock-test-3'
        scheduler = RQScheduler([name_1], self.connection)

        self.assertEqual(scheduler.acquire_locks(), {name_1})
        self.assertEqual(scheduler._acquired_locks, {name_1})
        self.assertEqual(scheduler.acquire_locks(), set([]))

        # Only name_2 is returned since name_1 is already locked
        scheduler = RQScheduler([name_1, name_2], self.connection)
        self.assertEqual(scheduler.acquire_locks(), {name_2})
        self.assertEqual(scheduler._acquired_locks, {name_2})

        # When a new lock is successfully acquired, _acquired_locks is added
        scheduler._queue_names.add(name_3)
        self.assertEqual(scheduler.acquire_locks(), {name_3})
        self.assertEqual(scheduler._acquired_locks, {name_2, name_3})

    def test_lock_acquisition_with_auto_start(self):
        """Test lock acquisition with auto_start=True"""
        scheduler = RQScheduler(['auto-start'], self.connection)
        with mock.patch.object(scheduler, 'start') as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 1)

        # If process has started, scheduler.start() won't be called
        running_process = mock.MagicMock()
        running_process.is_alive.return_value = True
        scheduler = RQScheduler(['auto-start2'], self.connection)
        scheduler._process = running_process
        with mock.patch.object(scheduler, 'start') as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 0)
            self.assertEqual(running_process.is_alive.call_count, 1)

        # If the process has stopped for some reason, the scheduler should restart
        scheduler = RQScheduler(['auto-start3'], self.connection)
        stopped_process = mock.MagicMock()
        stopped_process.is_alive.return_value = False
        scheduler._process = stopped_process
        with mock.patch.object(scheduler, 'start') as mocked:
            scheduler.acquire_locks(auto_start=True)
            self.assertEqual(mocked.call_count, 1)
            self.assertEqual(stopped_process.is_alive.call_count, 1)

    def test_lock_release(self):
        """Test that scheduler.release_locks() only releases acquired locks"""
        name_1 = 'lock-test-1'
        name_2 = 'lock-test-2'
        scheduler_1 = RQScheduler([name_1], self.connection)

        self.assertEqual(scheduler_1.acquire_locks(), {name_1})
        self.assertEqual(scheduler_1._acquired_locks, {name_1})

        # Only name_2 is returned since name_1 is already locked
        scheduler_1_2 = RQScheduler([name_1, name_2], self.connection)
        self.assertEqual(scheduler_1_2.acquire_locks(), {name_2})
        self.assertEqual(scheduler_1_2._acquired_locks, {name_2})

        self.assertTrue(self.connection.exists(scheduler_1.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_2)))

        scheduler_1_2.release_locks()

        self.assertEqual(scheduler_1_2._acquired_locks, set())
        self.assertEqual(scheduler_1._acquired_locks, {name_1})

        self.assertTrue(self.connection.exists(scheduler_1.get_locking_key(name_1)))
        self.assertTrue(self.connection.exists(scheduler_1_2.get_locking_key(name_1)))
        self.assertFalse(self.connection.exists(scheduler_1_2.get_locking_key(name_2)))

    def test_queue_scheduler_pid(self):
        queue = Queue(connection=self.connection)
        scheduler = RQScheduler(
            [
                queue,
            ],
            connection=self.connection,
        )
        scheduler.acquire_locks()
        assert queue.scheduler_pid == os.getpid()

    def test_heartbeat(self):
        """Test that heartbeat updates locking keys TTL"""
        name_1 = 'lock-test-1'
        name_2 = 'lock-test-2'
        name_3 = 'lock-test-3'
        scheduler = RQScheduler([name_3], self.connection)
        scheduler.acquire_locks()
        scheduler = RQScheduler([name_1, name_2, name_3], self.connection)
        scheduler.acquire_locks()

        locking_key_1 = RQScheduler.get_locking_key(name_1)
        locking_key_2 = RQScheduler.get_locking_key(name_2)
        locking_key_3 = RQScheduler.get_locking_key(name_3)

        with self.connection.pipeline() as pipeline:
            pipeline.expire(locking_key_1, 1000)
            pipeline.expire(locking_key_2, 1000)
            pipeline.expire(locking_key_3, 1000)
            pipeline.execute()

        scheduler.heartbeat()
        self.assertEqual(self.connection.ttl(locking_key_1), 61)
        self.assertEqual(self.connection.ttl(locking_key_2), 61)
        self.assertEqual(self.connection.ttl(locking_key_3), 1000)

        # scheduler.stop() releases locks and sets status to STOPPED
        scheduler._status = scheduler.Status.WORKING
        scheduler.stop()
        self.assertFalse(self.connection.exists(locking_key_1))
        self.assertFalse(self.connection.exists(locking_key_2))
        self.assertTrue(self.connection.exists(locking_key_3))
        self.assertEqual(scheduler.status, scheduler.Status.STOPPED)

        # Heartbeat also works properly for schedulers with a single queue
        scheduler = RQScheduler([name_1], self.connection)
        scheduler.acquire_locks()
        self.connection.expire(locking_key_1, 1000)
        scheduler.heartbeat()
        self.assertEqual(self.connection.ttl(locking_key_1), 61)

    def test_enqueue_scheduled_jobs(self):
        """Scheduler can enqueue scheduled jobs"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        job = Job.create('myfunc', connection=self.connection)
        job.save()
        registry.schedule(job, datetime(2019, 1, 1, tzinfo=timezone.utc))
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

        # After job is scheduled, registry should be empty
        self.assertEqual(len(registry), 0)

        # Jobs scheduled in the far future should not be affected
        registry.schedule(job, datetime(2100, 1, 1, tzinfo=timezone.utc))
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

    def test_prepare_registries(self):
        """prepare_registries() creates self._scheduled_job_registries"""
        foo_queue = Queue('foo', connection=self.connection)
        bar_queue = Queue('bar', connection=self.connection)
        scheduler = RQScheduler([foo_queue, bar_queue], connection=self.connection)
        self.assertEqual(scheduler._scheduled_job_registries, [])
        scheduler.prepare_registries([foo_queue.name])
        self.assertEqual(scheduler._scheduled_job_registries, [ScheduledJobRegistry(queue=foo_queue)])
        scheduler.prepare_registries([foo_queue.name, bar_queue.name])
        self.assertEqual(
            scheduler._scheduled_job_registries,
            [ScheduledJobRegistry(queue=foo_queue), ScheduledJobRegistry(queue=bar_queue)],
        )


class TestWorker(RQTestCase):
    def test_work_burst(self):
        """worker.work() with scheduler enabled works properly"""
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)
        worker.work(burst=True, with_scheduler=False)
        self.assertIsNone(worker.scheduler)

        worker = Worker(queues=[queue], connection=self.connection)
        worker.work(burst=True, with_scheduler=True)
        self.assertIsNotNone(worker.scheduler)
        self.assertIsNone(self.connection.get(worker.scheduler.get_locking_key('default')))

    @mock.patch.object(RQScheduler, 'acquire_locks')
    def test_run_maintenance_tasks(self, mocked):
        """scheduler.acquire_locks() is called only when scheduled is enabled"""
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)

        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 0)

        # if scheduler object exists and it's a first start, acquire locks should not run
        worker.last_cleaned_at = None
        worker.scheduler = RQScheduler([queue], connection=self.connection)
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 0)

        # the scheduler exists and it's NOT a first start, since the process doesn't exists,
        # should call acquire_locks to start the process
        worker.last_cleaned_at = datetime.now()
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 1)

        # the scheduler exists, the process exists, but the process is not alive
        running_process = mock.MagicMock()
        running_process.is_alive.return_value = False
        worker.scheduler._process = running_process
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(running_process.is_alive.call_count, 1)

        # the scheduler exists, the process exits, and it is alive. acquire_locks shouldn't run
        running_process.is_alive.return_value = True
        worker.run_maintenance_tasks()
        self.assertEqual(mocked.call_count, 2)
        self.assertEqual(running_process.is_alive.call_count, 2)

    def test_work(self):
        queue = Queue(connection=self.connection)
        worker = Worker(queues=[queue], connection=self.connection)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)

    @ssl_test
    def test_work_with_ssl(self):
        connection = find_empty_redis_database(ssl=True)
        queue = Queue(connection=connection)
        worker = Worker(queues=[queue], connection=connection)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)

    def test_work_with_serializer(self):
        queue = Queue(connection=self.connection, serializer=JSONSerializer)
        worker = Worker(queues=[queue], connection=self.connection, serializer=JSONSerializer)
        p = Process(target=kill_worker, args=(os.getpid(), False, 5))

        p.start()
        queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello, meta={'foo': 'bar'})
        worker.work(burst=False, with_scheduler=True)
        p.join(1)
        self.assertIsNotNone(worker.scheduler)
        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(len(registry), 1)


class TestQueue(RQTestCase):
    def test_enqueue_at(self):
        """queue.enqueue_at() puts job in the scheduled"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        job = queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 1)

        # enqueue_at set job status to "scheduled"
        self.assertTrue(job.get_status() == 'scheduled')

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)
        self.assertEqual(len(registry), 0)

    def test_enqueue_at_at_front(self):
        """queue.enqueue_at() accepts at_front argument. When true, job will be put at position 0
        of the queue when the time comes for the job to be scheduled"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)
        scheduler = RQScheduler([queue], connection=self.connection)
        scheduler.acquire_locks()
        # Jobs created using enqueue_at is put in the ScheduledJobRegistry
        # job_first should be enqueued first
        job_first = queue.enqueue_at(datetime(2019, 1, 1, tzinfo=timezone.utc), say_hello)
        # job_second will be enqueued second, but "at_front"
        job_second = queue.enqueue_at(datetime(2019, 1, 2, tzinfo=timezone.utc), say_hello, at_front=True)
        self.assertEqual(len(queue), 0)
        self.assertEqual(len(registry), 2)

        # enqueue_at set job status to "scheduled"
        self.assertTrue(job_first.get_status() == 'scheduled')
        self.assertTrue(job_second.get_status() == 'scheduled')

        # After enqueue_scheduled_jobs() is called, the registry is empty
        # and job is enqueued
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 2)
        self.assertEqual(len(registry), 0)
        self.assertEqual(0, queue.get_job_position(job_second.id))
        self.assertEqual(1, queue.get_job_position(job_first.id))

    def test_enqueue_in(self):
        """queue.enqueue_in() schedules job correctly"""
        queue = Queue(connection=self.connection)
        registry = ScheduledJobRegistry(queue=queue)

        job = queue.enqueue_in(timedelta(seconds=30), say_hello)
        now = datetime.now(timezone.utc)
        scheduled_time = registry.get_scheduled_time(job)
        # Ensure that job is scheduled roughly 30 seconds from now
        self.assertTrue(now + timedelta(seconds=28) < scheduled_time < now + timedelta(seconds=32))

    def test_enqueue_in_with_retry(self):
        """Ensure that the retry parameter is passed
        to the enqueue_at function from enqueue_in.
        """
        queue = Queue(connection=self.connection)
        job = queue.enqueue_in(timedelta(seconds=30), say_hello, retry=Retry(3, [2]))
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [2])

    def test_custom_connection_pool(self):
        """Connection pool customizing. Ensure that we can properly set a
        custom connection pool class and pass extra arguments"""
        custom_conn = redis.Redis(
            connection_pool=redis.ConnectionPool(
                connection_class=CustomRedisConnection,
                db=4,
                custom_arg="foo",
            )
        )

        queue = Queue(connection=custom_conn)
        scheduler = RQScheduler([queue], connection=custom_conn)

        scheduler_connection = scheduler.connection.connection_pool.get_connection('info')

        self.assertEqual(scheduler_connection.__class__, CustomRedisConnection)
        self.assertEqual(scheduler_connection.get_custom_arg(), "foo")

    def test_no_custom_connection_pool(self):
        """Connection pool customizing must not interfere if we're using a standard
        connection (non-pooled)"""
        standard_conn = redis.Redis(db=5)

        queue = Queue(connection=standard_conn)
        scheduler = RQScheduler([queue], connection=standard_conn)

        scheduler_connection = scheduler.connection.connection_pool.get_connection('info')

        self.assertEqual(scheduler_connection.__class__, redis.Connection)
