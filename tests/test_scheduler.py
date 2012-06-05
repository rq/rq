from datetime import datetime, timedelta

from rq import Queue, Worker
from rq.job import Job
from rq.scheduler import Scheduler

from tests import RQTestCase
from tests.fixtures import say_hello


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

    def test_create_scheduled_job(self):
        """
        Ensure that scheduled jobs are created, put in the scheduler queue
        and have the right score
        """
        scheduled_time = datetime.now()
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.schedule(scheduled_time, say_hello)
        self.assertEqual(job, Job.fetch(job.id, connection=self.testconn))
        self.assertIn(job.id, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 1))
        self.assertEqual(self.testconn.zscore(scheduler.scheduled_jobs_key, job.id),
                         int(scheduled_time.strftime('%s')))

    def test_get_jobs_to_queue(self):
        """
        Ensure that jobs scheduled the future are not queued.
        """
        now = datetime.now()
        scheduler = Scheduler(connection=self.testconn)
        job = scheduler.schedule(now, say_hello)
        self.assertIn(job, scheduler.get_jobs_to_queue())
        future_time = now + timedelta(hours=1)
        job = scheduler.schedule(future_time, say_hello)
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
        job = scheduler.schedule(now, say_hello)
        scheduler.enqueue_job(job)
        self.assertNotIn(job, self.testconn.zrange(scheduler.scheduled_jobs_key, 0, 10))
        job = Job.fetch(job.id, connection=self.testconn)
        self.assertTrue(job.enqueued_at is not None)
        queue = scheduler.get_queue_for_job(job)
        self.assertIn(job, queue.jobs)
        queue = Queue.from_queue_key('rq:queue:{0}'.format(queue_name))
        self.assertIn(job, queue.jobs)
