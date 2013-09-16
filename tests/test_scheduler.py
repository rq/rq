from datetime import datetime, timedelta
import os
import signal
import time
import times
from threading import Thread

from rq import Queue
from rq.compat import as_text
from rq.job import Job
from rq.scheduler import Scheduler

from tests import RQTestCase


def say_hello(name=None):
    """A job with a single argument and a return value."""
    if name is None:
        name = 'Stranger'
    return 'Hi there, %s!' % (name,)


def simple_addition(x, y, z):
    return x + y + z


class TestScheduler(RQTestCase):

    def setUp(self):
        super(TestScheduler, self).setUp()
        self.scheduler = Scheduler(connection=self.testconn)

    def test_birth_and_death_registration(self):
        """
        When scheduler registers it's birth, besides creating a key, it should
        also set an expiry that's a few seconds longer than it's polling
        interval so it automatically expires if scheduler is unexpectedly
        terminated.
        """
        key = Scheduler.scheduler_key
        self.assertNotIn(key, self.testconn.keys('*'))
        scheduler = Scheduler(connection=self.testconn, interval=20)
        scheduler.register_birth()
        results = [as_text(key) for key in self.testconn.keys('*')]
        self.assertIn(key, results)
        self.assertEqual(self.testconn.ttl(key), 30)
        self.assertFalse(self.testconn.hexists(key, 'death'))
        self.assertRaises(ValueError, scheduler.register_birth)
        scheduler.register_death()
        self.assertTrue(self.testconn.hexists(key, 'death'))

    def test_create_job(self):
        """
        Ensure that jobs are created properly.
        """
        job = self.scheduler._create_job(say_hello, args=(), kwargs={})
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job, job_from_queue)
        self.assertEqual(job_from_queue.func, say_hello)

    def test_job_not_persisted_if_commit_false(self):
        """
        Ensure jobs are only saved to Redis if commit=True.
        """
        job = self.scheduler._create_job(say_hello, commit=False)
        self.assertEqual(self.testconn.hgetall(job.key), {})

    def test_create_scheduled_job(self):
        """
        Ensure that scheduled jobs are put in the scheduler queue with the right score
        """
        scheduled_time = times.now()
        job = self.scheduler.enqueue_at(scheduled_time, say_hello)
        self.assertEqual(job, Job.fetch(job.id, connection=self.testconn))
        results = [as_text(key) for key in
                   self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)]
        self.assertIn(job.id, results)
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(scheduled_time))

    def test_enqueue_in(self):
        """
        Ensure that jobs have the right scheduled time.
        """
        right_now = times.now()
        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        results = [as_text(key) for key in
                   self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)]
        self.assertIn(job.id, results)
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(right_now + time_delta))
        time_delta = timedelta(hours=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(right_now + time_delta))

    def test_get_jobs(self):
        """
        Ensure get_jobs() returns all jobs until the specified time.
        """
        now = times.now()
        job = self.scheduler.enqueue_at(now, say_hello)        
        self.assertIn(job, self.scheduler.get_jobs(now))
        future_time = now + timedelta(hours=1)
        job = self.scheduler.enqueue_at(future_time, say_hello)
        self.assertIn(job, self.scheduler.get_jobs(timedelta(hours=1, seconds=1)))
        self.assertIn(job, [j[0] for j in self.scheduler.get_jobs(with_times=True)])
        self.assertIsInstance(self.scheduler.get_jobs(with_times=True)[0][1], datetime)
        self.assertNotIn(job, self.scheduler.get_jobs(timedelta(minutes=59, seconds=59)))

    def test_get_jobs_to_queue(self):
        """
        Ensure that jobs scheduled the future are not queued.
        """
        now = times.now()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler.get_jobs_to_queue())
        future_time = now + timedelta(hours=1)
        job = self.scheduler.enqueue_at(future_time, say_hello)
        self.assertNotIn(job, self.scheduler.get_jobs_to_queue())

    def test_enqueue_job(self):
        """
        When scheduled job is enqueued, make sure:
        - Job is removed from the sorted set of scheduled jobs
        - "enqueued_at" attribute is properly set
        - Job appears in the right queue
        """
        now = times.now()
        queue_name = 'foo'
        scheduler = Scheduler(connection=self.testconn, queue_name=queue_name)

        job = scheduler.enqueue_at(now, say_hello)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 10))
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.enqueued_at is not None)
        queue = scheduler.get_queue_for_job(job)
        self.assertIn(job, queue.jobs)
        queue = Queue.from_queue_key('rq:queue:{0}'.format(queue_name))
        self.assertIn(job, queue.jobs)

    def test_job_membership(self):
        now = times.now()
        job = self.scheduler.enqueue_at(now, say_hello)
        self.assertIn(job, self.scheduler)
        self.assertIn(job.id, self.scheduler)
        self.assertIn(say_hello, self.scheduler)
        self.assertNotIn("non-existing-job-id", self.scheduler)
        self.assertNotIn(times.now, self.scheduler)

    def test_cancel_scheduled_job(self):
        """
        When scheduled job is canceled, make sure:
        - Job is removed from the sorted set of scheduled jobs
        - Canceling jobs using job id works
        - Canceling a job that has been deleted doesn't cause an error
        """
        # schedule a job to be enqueued one minute from now
        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        # cancel the scheduled job and check that it's gone from the set
        self.scheduler.cancel(job)
        self.assertNotIn(job.id, self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1))

        # Cancel using job id
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        self.scheduler.cancel(job.id)
        self.assertNotIn(job.id, self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1))

        # Cancel a deleted job doesn't cause an exception
        job = self.scheduler.enqueue_in(time_delta, say_hello)
        job.delete()
        self.scheduler.cancel(job.id)

    def test_change_execution_time(self):
        """
        Ensure ``change_execution_time`` is called, ensure that job's score is updated
        """
        job = self.scheduler.enqueue_at(times.now(), say_hello)
        new_date = datetime(2010, 1, 1)
        self.scheduler.change_execution_time(job, new_date)
        self.assertEqual(times.to_unix(new_date),
            self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id))
        self.scheduler.cancel(job)
        self.assertRaises(ValueError, self.scheduler.change_execution_time, job, new_date)

    def test_args_kwargs_are_passed_correctly(self):
        """
        Ensure that arguments and keyword arguments are properly saved to jobs.
        """
        job = self.scheduler.enqueue_at(times.now(), simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = self.scheduler.enqueue_at(times.now(), simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = self.scheduler.enqueue_at(times.now(), simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

        time_delta = timedelta(minutes=1)
        job = self.scheduler.enqueue_in(time_delta, simple_addition, 1, 1, 1)
        self.assertEqual(job.args, (1, 1, 1))
        job = self.scheduler.enqueue_in(time_delta, simple_addition, z=1, y=1, x=1)
        self.assertEqual(job.kwargs, {'x': 1, 'y': 1, 'z': 1})
        job = self.scheduler.enqueue_in(time_delta, simple_addition, 1, z=1, y=1)
        self.assertEqual(job.kwargs, {'y': 1, 'z': 1})
        self.assertEqual(job.args, (1,))

    def test_interval_and_repeat_persisted_correctly(self):
        """
        Ensure that interval and repeat attributes get correctly saved in Redis.
        """
        job = self.scheduler.schedule(times.now(), say_hello, interval=10, repeat=11)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job_from_queue.meta['interval'], 10)
        self.assertEqual(job_from_queue.meta['repeat'], 11)

    def test_repeat_without_interval_raises_error(self):
        # Ensure that an error is raised if repeat is specified without interval
        def create_job():
            self.scheduler.schedule(times.now(), say_hello, repeat=11)
        self.assertRaises(ValueError, create_job)

    def test_job_with_intervals_get_rescheduled(self):
        """
        Ensure jobs with interval attribute are put back in the scheduler
        """
        time_now = times.now()
        interval = 10
        job = self.scheduler.schedule(time_now, say_hello, interval=interval)
        self.scheduler.enqueue_job(job)
        results = [as_text(key) for key in
                   self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)]
        self.assertIn(job.id, results)
        self.assertEqual(self.testconn.zscore(self.scheduler.scheduled_jobs_key, job.id),
                         times.to_unix(time_now) + interval)

    def test_job_with_repeat(self):
        """
        Ensure jobs with repeat attribute are put back in the scheduler
        X (repeat) number of times
        """
        time_now = times.now()
        interval = 10
        # If job is repeated once, the job shouldn't be put back in the queue
        job = self.scheduler.schedule(time_now, say_hello, interval=interval, repeat=1)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id,
            self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1))

        # If job is repeated twice, it should only be put back in the queue once
        job = self.scheduler.schedule(time_now, say_hello, interval=interval, repeat=2)
        self.scheduler.enqueue_job(job)
        results = [as_text(key) for key in
                   self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1)]
        self.assertIn(job.id, results)
        self.scheduler.enqueue_job(job)
        self.assertNotIn(job.id,
            self.testconn.zrange(self.scheduler.scheduled_jobs_key, 0, 1))

    def test_missing_jobs_removed_from_scheduler(self):
        """
        Ensure jobs that don't exist when queued are removed from the scheduler.
        """
        job = self.scheduler.schedule(times.now(), say_hello)
        job.cancel()
        self.scheduler.get_jobs_to_queue()
        self.assertNotIn(job.id, self.testconn.zrange(
            self.scheduler.scheduled_jobs_key, 0, 1))

    def test_periodic_jobs_sets_ttl(self):
        """
        Ensure periodic jobs set result_ttl to infinite.
        """
        job = self.scheduler.schedule(times.now(), say_hello, interval=5)
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertIsNone(job_from_queue.result_ttl)

    def test_run(self):
        """
        Check correct signal handling in Scheduler.run().
        """
        def send_stop_signal():
            """
            Sleep for 1 second, then send a INT signal to ourself, so the
            signal handler installed by scheduler.run() is called.
            """
            time.sleep(1)
            os.kill(os.getpid(), signal.SIGINT)
        thread = Thread(target=send_stop_signal)
        thread.start()
        self.assertRaises(SystemExit, self.scheduler.run)
        thread.join()

    def test_scheduler_w_o_explicit_connection(self):
        """
        Ensure instantiating Scheduler w/o explicit connection works.
        """
        s = Scheduler()
        self.assertEqual(s.connection, self.testconn)

    def test_no_functions_from__main__module(self):
        """
        Ensure functions from the __main__ module are not accepted for scheduling.
        """
        def dummy():
            return 1
        # Fake __main__ module function
        dummy.__module__ = "__main__"
        self.assertRaises(ValueError, self.scheduler._create_job, dummy)

    def test_burst_mode(self):
        """
        Ensure that scheduler on burst mode runs properly.
        """
        job = self.scheduler.schedule(times.now(), say_hello)
        queue = Queue(connection=self.testconn)
        self.assertNotIn(job, queue.jobs)
        self.scheduler.run(burst=True)
        self.assertIn(job, queue.jobs)
