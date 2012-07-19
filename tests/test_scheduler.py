from datetime import datetime, timedelta

from rq import Queue, Worker
from rq.job import Job
from rq_scheduler import Scheduler

from tests import RQTestCase


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def simple_addition(x, y, z):
    return x + y + z


class TestScheduler(RQTestCase):

    def test_birth_and_death_registration(self):
        key = Scheduler.scheduler_key
        self.assertNotIn(key, self.testconn.keys('*'))
        scheduler = Scheduler(connection=self.testconn)
        scheduler.register_birth()
        self.assertIn(key, self.testconn.keys('*'))
        self.assertFalse(self.testconn.hexists(key, 'death'))
        self.assertRaises(ValueError, scheduler.register_birth)
        scheduler.register_death()
        self.assertTrue(self.testconn.hexists(key, 'death'))

    def test_create_job(self):
        """
        Ensure that jobs are created properly.
        """
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler._create_job(say_hello, args=[], kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job, job_from_queue)
        self.assertEqual(job_from_queue.func, say_hello)

    def test_job_not_persisted_if_commit_false(self):
        """
        Ensure jobs are only saved to Redis if commit=True.
        """
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler._create_job(say_hello, args=[], kwargs={}, commit=False)
        self.assertEqual(self.testconn.hgetall(job.key), {})

    def test_create_scheduled_job(self):
        """
        Ensure that scheduled jobs are put in the scheduler queue with the right score
        """
        scheduled_time = datetime.now()
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_at(scheduled_time, say_hello)
        self.assertEqual(job, Job.fetch(job.id, connection=self.testconn))
        self.assertIn(job.id, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 1))
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         int(scheduled_time.strftime('%s')))

    def test_enqueue_in(self):
        """
        Ensure that jobs have the right scheduled time.
        """
        right_now = datetime.now()
        time_delta = timedelta(minutes=1)
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_in(time_delta, say_hello)
        self.assertIn(job.id, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 1))
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         int((right_now + time_delta).strftime('%s')))
        time_delta = timedelta(hours=1)
        job = scheduler.enqueue_in(time_delta, say_hello)
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         int((right_now + time_delta).strftime('%s')))

    def test_get_jobs_to_queue(self):
        """
        Ensure that jobs scheduled the future are not queued.
        """
        now = datetime.now()
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, scheduler.get_jobs_to_queue())
        future_time = now + timedelta(hours=1)
        job = scheduler.enqueue_at(future_time, say_hello)
        self.assertNotIn(job, scheduler.get_jobs_to_queue())

    def test_enqueue_job(self):
        """
        When scheduled job is enqueued, make sure:
        - Job is removed from the sorted set of scheduled jobs
        - "enqueued_at" attribute is properly set
        - Job appears in the right queue
        """
        now = datetime.now()
        queue_name = 'foo'
        scheduler = Scheduler(connection=self.testconn, queue_name=queue_name)
        job = scheduler.enqueue_at(now, say_hello)
        scheduler.enqueue_job(job)
        self.assertNotIn(job, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 10))
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.enqueued_at is not None)
        queue = scheduler.get_queue_for_job(job)
        self.assertIn(job, queue.jobs)
        queue = Queue.from_queue_key('rq:queue:{0}'.format(queue_name))
        self.assertIn(job, queue.jobs)

    def test_cancel_scheduled_job(self):
        """
        When scheduled job is canceled, make sure:
        - Job is removed from the sorted set of scheduled jobs
        """
        # schedule a job to be enqueued one minute from now
        time_delta = timedelta(minutes=1)
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_in(time_delta, say_hello)
        # cancel the scheduled job and check that it's gone from the set
        scheduler.cancel(job)
        self.assertNotIn(job.id, self.testconn.zrange(
            scheduler.scheduled_jobs_key, 0, 1))

    def test_change_execution_time(self):
        """
        Ensure ``change_execution_time`` is called, ensure that job's score is updated
        """
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_at(datetime.now(), say_hello)
        new_date = datetime(2010, 1, 1)
        scheduler.change_execution_time(job, new_date)
        self.assertEqual(int(new_date.strftime('%s')),
            self.testconn.zscore(scheduler.scheduled_jobs_key, job.id))

    def test_args_kwargs_are_passed_correctly(self):
        """
        Ensure that arguments and keyword arguments are properly saved to jobs.
        """
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue_at(datetime.now(), simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = scheduler.enqueue_at(datetime.now(), simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = scheduler.enqueue_at(datetime.now(), simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

        time_delta = timedelta(minutes=1)
        job = scheduler.enqueue_in(time_delta, simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = scheduler.enqueue_in(time_delta, simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = scheduler.enqueue_in(time_delta, simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

    def test_interval_and_repeat_persisted_correctly(self):
        """
        Ensure that interval and repeat attributes get correctly saved in Redis.
        """
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.enqueue(datetime.now(), say_hello, interval=10, repeat=11)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(int(job_from_queue.interval), 10)
        self.assertEqual(int(job_from_queue.repeat), 11)
