from datetime import timedelta

from rq import Queue, Worker
from rq.job import Job
from rq.registry import ScheduledJobRegistry
from rq.repeat import Repeat
from rq.utils import now
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class TestRepeat(RQTestCase):
    """Tests for the Repeat class"""

    def test_repeat_class_initialization(self):
        """Repeat correctly parses `times` and `interval` parameters"""
        # Test with single interval value
        repeat = Repeat(times=3, interval=5)
        self.assertEqual(repeat.times, 3)
        self.assertEqual(repeat.intervals, [5])

        # Test with list of intervals
        repeat = Repeat(times=4, interval=[5, 10, 15])
        self.assertEqual(repeat.times, 4)
        self.assertEqual(repeat.intervals, [5, 10, 15])

        # Test validation errors
        # times must be at least 1
        self.assertRaises(ValueError, Repeat, times=0, interval=5)
        self.assertRaises(ValueError, Repeat, times=-1, interval=5)

        # interval can't be negative
        self.assertRaises(ValueError, Repeat, times=1, interval=-5)
        self.assertRaises(ValueError, Repeat, times=3, interval=[5, -10])

        # interval must be int or iterable
        self.assertRaises(TypeError, Repeat, times=3, interval='not_a_number')

    def test_get_interval(self):
        """get_interval() returns the right repeat interval"""
        # Test with intervals list shorter than needed
        intervals = [5, 10, 15]

        # First interval
        self.assertEqual(Repeat.get_interval(0, intervals), 5)

        # Second interval
        self.assertEqual(Repeat.get_interval(1, intervals), 10)

        # Third interval
        self.assertEqual(Repeat.get_interval(2, intervals), 15)

        # Beyond the list length, should return the last interval
        self.assertEqual(Repeat.get_interval(3, intervals), 15)

        # Test with single interval
        intervals = [7]
        self.assertEqual(Repeat.get_interval(0, intervals), 7)
        self.assertEqual(Repeat.get_interval(1, intervals), 7)

        # Test with empty intervals list (edge case that shouldn't happen in practice)
        # This would cause an IndexError in get_interval
        with self.assertRaises(IndexError):
            Repeat.get_interval(0, [])

    def test_persistence_of_repeat_data(self):
        """Repeat related data is stored and restored properly"""
        # Create a job with repeat settings
        job = Job.create(func=say_hello, connection=self.connection)
        job.repeats_left = 3
        job.repeat_intervals = [5, 10, 15]
        job.save()

        # Clear the job's local state and reload from Redis
        job.repeats_left = None
        job.repeat_intervals = None
        job.refresh()

        # Verify the repeat settings were restored correctly
        self.assertEqual(job.repeats_left, 3)
        self.assertEqual(job.repeat_intervals, [5, 10, 15])


class TestRepeatEnqueue(RQTestCase):
    """Test that Repeat objects are correctly handled when enqueuing jobs"""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)

    def test_enqueue_with_repeat(self):
        """Test that repeat parameters are stored when enqueuing a job with Repeat object"""
        repeat = Repeat(times=3, interval=[5, 10, 15])

        # Enqueue a job with a Repeat object
        job = self.queue.enqueue(say_hello, repeat=repeat)

        # Verify the job has the repeat attributes set correctly
        self.assertEqual(job.repeats_left, 3)
        self.assertEqual(job.repeat_intervals, [5, 10, 15])

        # Verify the job is persisted with the repeat attributes
        job_id = job.id
        loaded_job = Job.fetch(job_id, connection=self.connection)
        self.assertEqual(loaded_job.repeats_left, 3)
        self.assertEqual(loaded_job.repeat_intervals, [5, 10, 15])

    def test_enqueue_at_with_repeat(self):
        """Test that repeat parameters are stored when using enqueue_at with Repeat object"""
        repeat = Repeat(times=2, interval=[60, 120])

        # Use enqueue_at method
        job = self.queue.enqueue_at(now(), say_hello, repeat=repeat)

        # Verify the job has the repeat attributes set correctly
        self.assertEqual(job.repeats_left, 2)
        self.assertEqual(job.repeat_intervals, [60, 120])

    def test_enqueue_many_with_repeat(self):
        """Test that repeat parameters are stored when using enqueue_many with Repeat objects"""
        # Create different Repeat objects

        # Prepare job data with repeat parameters
        job_data1 = self.queue.prepare_data(func=say_hello, repeat=Repeat(times=3, interval=10))

        job_data2 = self.queue.prepare_data(func=say_hello, repeat=Repeat(times=2, interval=[30, 60]))

        # Enqueue multiple jobs
        job_1, job_2 = self.queue.enqueue_many([job_data1, job_data2])

        job_1.refresh()
        self.assertEqual(job_1.repeats_left, 3)
        self.assertEqual(job_1.repeat_intervals, [10])

        self.assertEqual(job_2.repeats_left, 2)
        self.assertEqual(job_2.repeat_intervals, [30, 60])

    def test_repeat_schedule_interval_zero(self):
        """Test the Repeat.schedule method properly schedules job repeats"""

        # Test 1: Job with zero interval should be executed immediately
        repeat = Repeat(times=2, interval=0)
        job = self.queue.enqueue(say_hello, repeat=repeat)

        Repeat.schedule(job, self.queue)

        # Job was enqueued immediately since interval is 0
        self.assertIn(job.id, self.queue.get_job_ids())
        job.refresh()
        self.assertEqual(job.repeats_left, 1)

        Repeat.schedule(job, self.queue)
        self.assertEqual(job.repeats_left, 0)

        # Job can only be repeated twice
        with self.assertRaises(ValueError):
            Repeat.schedule(job, self.queue)

    def test_repeat_schedule_interval_greater_than_zero(self):
        """Test the Repeat.schedule method properly schedules job repeats"""

        queue = self.queue
        registry = ScheduledJobRegistry(queue=queue)

        repeat = Repeat(times=3, interval=30)  # 30 second interval
        job = queue.enqueue(say_hello, repeat=repeat)

        # Clear the queue so we can verify the job is not enqueued immediately
        queue.empty()
        # Get current time for reference
        before_schedule = now()

        # Schedule the job
        Repeat.schedule(job, queue)

        after_schedule = now()

        # Verify job was not enqueued immediately
        self.assertNotIn(job.id, queue.get_job_ids())

        # Verify job was added to scheduled registry
        self.assertIn(job.id, registry.get_job_ids())

        # Scheduled time should be approximately 30 seconds from now
        scheduled_time = registry.get_scheduled_time(job.id)
        expected_min = before_schedule + timedelta(seconds=25)  # Allow 1 sec buffer
        expected_max = after_schedule + timedelta(seconds=35)  # Allow 1 sec buffer

        self.assertTrue(
            expected_min <= scheduled_time <= expected_max,
            f'Job not scheduled in expected window: {expected_min} <= {scheduled_time} <= {expected_max}',
        )

        # Check repeats_left was decremented
        job.refresh()
        self.assertEqual(job.repeats_left, 2)


class TestWorkerRepeat(RQTestCase):
    def test_successful_job_repeat(self):
        """Test that successful jobs are repeated according to Repeat settings"""
        queue = Queue(connection=self.connection)

        job = queue.enqueue(say_hello, repeat=Repeat(times=1))

        worker = Worker([queue], connection=self.connection)
        worker.work(burst=True, max_jobs=1)

        # The original job should have been processed and repeated
        self.assertIn(job.id, queue.get_job_ids())

        worker = Worker([queue], connection=self.connection)
        worker.work(burst=True, max_jobs=1)

        # No repeats left
        self.assertNotIn(job.id, queue.get_job_ids())

        # Failed jobs don't trigger repeats
        job = queue.enqueue(div_by_zero, repeat=Repeat(times=1))

        self.assertEqual(job.repeats_left, 1)
        worker.work(burst=True)
        # Job shouldn't be repeated
        self.assertNotIn(job.id, queue.get_job_ids())
