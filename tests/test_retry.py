import time
from datetime import datetime, timedelta, timezone

from rq import Queue
from rq.executions import prepare_execution
from rq.job import Job, JobStatus, Retry
from rq.registry import FailedJobRegistry, StartedJobRegistry
from rq.results import Result
from rq.scheduler import RQScheduler
from rq.worker import Worker
from tests import RQTestCase, slow
from tests.fixtures import div_by_zero, say_hello


def return_retry(max: int = 1, interval: int = 0):
    return Retry(max=max, interval=interval)


class TestRetry(RQTestCase):
    """Tests from test_retry.py"""

    def test_persistence_of_retry_data(self):
        """Retry related data is stored and restored properly"""
        job = Job.create(func=say_hello, connection=self.connection)
        job.retries_left = 3
        job.retry_intervals = [1, 2, 3]
        job.enqueue_at_front_on_retry = True
        job.save()

        job.retries_left = None
        job.retry_intervals = None
        job.enqueue_at_front_on_retry = False
        job.refresh()
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [1, 2, 3])
        self.assertEqual(job.enqueue_at_front_on_retry, True)

    def test_retry_class(self):
        """Retry parses `max` and `interval` correctly"""
        retry = Retry(max=1)
        self.assertEqual(retry.max, 1)
        self.assertEqual(retry.intervals, [0])
        self.assertEqual(retry.enqueue_at_front, False)
        self.assertRaises(ValueError, Retry, max=0)

        retry = Retry(max=2, interval=5)
        self.assertEqual(retry.max, 2)
        self.assertEqual(retry.intervals, [5])

        retry = Retry(max=3, interval=[5, 10])
        self.assertEqual(retry.max, 3)
        self.assertEqual(retry.intervals, [5, 10])

        retry = Retry(max=1, enqueue_at_front=True)
        self.assertEqual(retry.max, 1)
        self.assertEqual(retry.intervals, [0])
        self.assertEqual(retry.enqueue_at_front, True)

        # interval can't be negative
        self.assertRaises(ValueError, Retry, max=1, interval=-5)
        self.assertRaises(ValueError, Retry, max=1, interval=[1, -5])

    def test_retry_repr(self):
        """Retry repr is stable and human-readable"""
        self.assertEqual(repr(Retry(max=1)), 'Retry(max=1, interval=0, enqueue_at_front=False)')
        self.assertEqual(repr(Retry(max=2, interval=5)), 'Retry(max=2, interval=5, enqueue_at_front=False)')
        self.assertEqual(
            repr(Retry(max=3, interval=[5, 10])),
            'Retry(max=3, interval=[5, 10], enqueue_at_front=False)',
        )
        self.assertEqual(
            repr(Retry(max=1, enqueue_at_front=True)),
            'Retry(max=1, interval=0, enqueue_at_front=True)',
        )

    def test_get_retry_interval(self):
        """get_retry_interval() returns the right retry interval"""
        job = Job.create(func=say_hello, connection=self.connection)

        # Handle case where self.retry_intervals is None
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 0)

        # Handle the most common case
        job.retry_intervals = [1, 2]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 2)

        # Handle cases where number of retries > length of interval
        job.retries_left = 4
        job.retry_intervals = [1, 2, 3]
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 3
        self.assertEqual(job.get_retry_interval(), 1)
        job.retries_left = 2
        self.assertEqual(job.get_retry_interval(), 2)
        job.retries_left = 1
        self.assertEqual(job.get_retry_interval(), 3)

    def test_job_retry(self):
        """job.retry() works properly"""
        queue = Queue(connection=self.connection)
        retry = Retry(max=3, interval=5)
        job = queue.enqueue(div_by_zero, retry=retry)

        with self.connection.pipeline() as pipeline:
            job.retry(queue, pipeline)
            pipeline.execute()

        self.assertEqual(job.retries_left, 2)
        # status should be scheduled since it's retried with 5 seconds interval
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)

        retry = Retry(max=3)
        job = queue.enqueue(div_by_zero, retry=retry)

        with self.connection.pipeline() as pipeline:
            job.retry(queue, pipeline)
            pipeline.execute()

        self.assertEqual(job.retries_left, 2)
        # status should be queued
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        retry = Retry(max=3, enqueue_at_front=True)
        job = queue.enqueue(div_by_zero, retry=retry)

        with self.connection.pipeline() as pipeline:
            job.retry(queue, pipeline)
            pipeline.execute()

        self.assertEqual(job.retries_left, 2)
        # status should be queued
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

    def test_retry_interval(self):
        """Retries with intervals are scheduled"""
        connection = self.connection
        queue = Queue(connection=connection)
        retry = Retry(max=1, interval=5)
        job = queue.enqueue(div_by_zero, retry=retry)

        worker = Worker([queue])
        registry = queue.scheduled_job_registry
        # If job if configured to retry with interval, it will be scheduled,
        # not directly put back in the queue
        queue.empty()
        worker.handle_job_failure(job, queue)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        self.assertEqual(job.retries_left, 0)
        self.assertEqual(len(registry), 1)
        self.assertEqual(queue.job_ids, [])
        # Scheduled time is roughly 5 seconds from now
        scheduled_time = registry.get_scheduled_time(job)
        now = datetime.now(timezone.utc)
        self.assertTrue(now + timedelta(seconds=3) < scheduled_time < now + timedelta(seconds=10))

    def test_cleanup_handles_retries(self):
        """Expired jobs should also be retried"""
        queue = Queue(connection=self.connection)
        registry = StartedJobRegistry(connection=self.connection)
        failed_job_registry = FailedJobRegistry(connection=self.connection)
        job = queue.enqueue(say_hello, retry=Retry(max=1))

        # Add job to StartedJobRegistry with past expiration time
        self.connection.zadd(registry.key, {job.id: 2})

        registry.cleanup()
        self.assertEqual(len(queue), 2)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        self.assertNotIn(job, failed_job_registry)

        self.connection.zadd(registry.key, {job.id: 2})
        # Job goes to FailedJobRegistry because it's only retried once
        registry.cleanup()
        self.assertEqual(len(queue), 2)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job, failed_job_registry)

    def test_retry_get_interval(self):
        """Retry.get_interval() returns the right retry interval"""
        self.assertEqual(Retry.get_interval(0, [1, 2, 3]), 1)
        self.assertEqual(Retry.get_interval(1, [1, 2, 3]), 2)
        self.assertEqual(Retry.get_interval(3, [1, 2, 3]), 3)
        self.assertEqual(Retry.get_interval(4, [1, 2, 3]), 3)
        self.assertEqual(Retry.get_interval(5, [1, 2, 3]), 3)

        # Handle case where interval is None
        self.assertEqual(Retry.get_interval(1, None), 0)
        self.assertEqual(Retry.get_interval(2, None), 0)

        # Handle case where interval is a single integer
        self.assertEqual(Retry.get_interval(1, 3), 3)
        self.assertEqual(Retry.get_interval(2, 3), 3)

    def test_handle_retry_result(self):
        """_handle_retry_result() increments number_of_retries, creates result, and enqueues or schedules"""
        queue = Queue(connection=self.connection)

        started = datetime.now(timezone.utc)
        ended = started + timedelta(seconds=1)

        # Test immediate retry (no interval)
        retry = Retry(max=2)
        job = queue.enqueue(say_hello)
        with self.connection.pipeline() as pipeline:
            job._handle_retry_result(
                queue,
                pipeline,
                retry=retry,
                execution_id='exec-1',
                execution_started_at=started,
                execution_ended_at=ended,
            )
            pipeline.execute()
        job.refresh()
        self.assertEqual(job.number_of_retries, 1)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.RETRIED)
        self.assertEqual(result.execution_id, 'exec-1')
        self.assertIsInstance(result.return_value, Retry)

        # Test scheduled retry (with interval)
        retry = Retry(max=2, interval=10)
        job = queue.enqueue(say_hello)
        with self.connection.pipeline() as pipeline:
            job._handle_retry_result(
                queue,
                pipeline,
                retry=retry,
                execution_id='exec-2',
                execution_started_at=started,
                execution_ended_at=ended,
            )
            pipeline.execute()
        job.refresh()
        self.assertEqual(job.number_of_retries, 1)
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.RETRIED)
        self.assertEqual(result.execution_id, 'exec-2')
        self.assertIsInstance(result.return_value, Retry)


class TestWorkerRetry(RQTestCase):
    """Tests from test_job_retry.py"""

    def test_handle_job_retry_max_retries_exceeded(self):
        """handle_job_retry() records a terminal max retries exceeded result"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, failure_ttl=5)
        worker = Worker([queue], connection=self.connection)
        worker.register_birth()

        retry = Retry(max=1)
        job.started_at = datetime.now(timezone.utc)
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        job.number_of_retries = 1

        # Mirror the real worker flow, which sets worker.execution before handle_job_retry.
        execution = prepare_execution(worker, job)

        worker.handle_job_retry(
            job=job,
            queue=queue,
            retry=retry,
            started_job_registry=StartedJobRegistry(connection=self.connection),
            execution=execution,
        )

        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.MAX_RETRIES_EXCEEDED)
        self.assertIsNone(result.exc_string)
        self.assertIsInstance(result.return_value, Retry)
        self.assertEqual(result.execution_id, execution.id)
        self.assertEqual(result.execution_ended_at, job.ended_at)
        self.assertTrue(0 < self.connection.ttl(job.key) <= job.failure_ttl)
        self.assertTrue(0 < self.connection.ttl(Result.get_key(job.id)) <= job.failure_ttl)

    def test_retry(self):
        """Worker processes retry correctly when job returns Retry"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry)
        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1)

        # A result with type `RETRIED` should be created
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.RETRIED)
        self.assertEqual(job.get_status(), JobStatus.QUEUED)

        # Retried result carries execution metadata populated by the worker.
        self.assertIsNotNone(result.execution_id)
        self.assertIsNotNone(result.execution_started_at)
        self.assertIsNotNone(result.execution_ended_at)

    def test_job_handle_retry(self):
        """handle_job_retry() increments job.number_of_retries"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry)
        worker = Worker([queue], connection=self.connection)

        # First retry should set number_of_retries to 1
        worker.work(max_jobs=1)
        job.refresh()
        self.assertEqual(job.number_of_retries, 1)

    def test_job_handle_retry_with_interval_increments_number_of_retries(self):
        """handle_job_retry() increments number_of_retries even when retry has an interval"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry, max=2, interval=10)
        worker = Worker([queue], connection=self.connection)

        worker.work(max_jobs=1)
        job.refresh()
        self.assertEqual(job.number_of_retries, 1)
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)

    def test_worker_handles_max_retry(self):
        """Job fails after maximum retries are exhausted"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry, max=2)
        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1)

        # A result with type `RETRIED` should be created,
        # job should be back in the queue
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.RETRIED)
        self.assertIn(job.id, queue.get_job_ids())

        # Second retry
        worker.work(max_jobs=1)
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.RETRIED)
        self.assertIn(job.id, queue.get_job_ids())

        # Third execution would fail since max number of retries is 2
        worker.work(max_jobs=1)
        result = job.latest_result()
        self.assertEqual(result.type, result.Type.MAX_RETRIES_EXCEEDED)
        self.assertIsNone(result.exc_string)
        self.assertIsInstance(result.return_value, Retry)
        self.assertNotIn(job.id, queue.get_job_ids())

    def test_worker_handles_retry_interval(self):
        """Worker handles retry with interval correctly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry, max=1, interval=10)
        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1)

        now = datetime.now(timezone.utc)
        # Job should be scheduled for retry
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        self.assertNotIn(job.id, queue.get_job_ids())
        registry = queue.scheduled_job_registry
        self.assertIn(job.id, registry)

        scheduled_time = registry.get_scheduled_time(job)
        # Ensure that job is scheduled roughly 5 seconds from now
        self.assertTrue(now + timedelta(seconds=7) < scheduled_time < now + timedelta(seconds=13))

        job = queue.enqueue(return_retry, max=1, interval=30)
        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1)

        now = datetime.now(timezone.utc)
        # Job should be scheduled for retry
        self.assertEqual(job.get_status(), JobStatus.SCHEDULED)
        self.assertNotIn(job.id, queue.get_job_ids())
        registry = queue.scheduled_job_registry
        self.assertIn(job.id, registry)

        scheduled_time = registry.get_scheduled_time(job)
        # Ensure that job is scheduled roughly 5 seconds from now
        self.assertTrue(now + timedelta(seconds=27) < scheduled_time < now + timedelta(seconds=33))

    def test_worker_handles_max_retry_with_interval(self):
        """Job fails after maximum retries are exhausted, even with retry interval"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(return_retry, max=1, interval=0)
        worker = Worker([queue], connection=self.connection)

        # First execution: job returns Retry, gets retried
        worker.work(max_jobs=1)
        job.refresh()
        self.assertEqual(job.number_of_retries, 1)
        self.assertIn(job.id, queue.get_job_ids())

        # Second execution: max retries exceeded, job should fail
        worker.work(max_jobs=1)
        job.refresh()
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertNotIn(job.id, queue.get_job_ids())

    def test_worker_handles_enqueue_at_front_on_retry(self):
        """Job is enqueued at front of the queue if enqueue_at_front_on_retry is True"""
        queue = Queue(connection=self.connection)
        retry = Retry(max=1, enqueue_at_front=True)
        job1 = queue.enqueue(div_by_zero, retry=retry)
        job2 = queue.enqueue(say_hello)

        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1)

        # job1 should be retried and enqueued at the front of the queue
        self.assertEqual(queue.job_ids, [job1.id, job2.id])

    @slow
    def test_worker_handles_enqueue_at_front_on_retry_with_interval(self):
        """Job is enqueued at front of the queue if enqueue_at_front_on_retry is True, even with retry interval"""
        queue = Queue(connection=self.connection)
        retry = Retry(max=1, interval=2, enqueue_at_front=True)

        job1 = queue.enqueue(div_by_zero, retry=retry)
        job2 = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.work(max_jobs=1, with_scheduler=True)  # schedules the retry
        # Confirm job was scheduled for retry
        self.assertEqual(job1.get_status(), JobStatus.SCHEDULED)
        scheduler = RQScheduler([queue.name], connection=self.connection, interval=1)
        scheduler.acquire_locks()
        scheduler.prepare_registries()

        # Poll scheduler until the scheduled retry is enqueued (timeout to avoid flakiness)
        deadline = time.time() + 5
        while time.time() < deadline:
            scheduler.enqueue_scheduled_jobs()
            if queue.job_ids == [job1.id, job2.id]:
                break
            time.sleep(0.1)

        self.assertEqual(queue.job_ids, [job1.id, job2.id])
