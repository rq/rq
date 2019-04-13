from rq import Queue
from rq.job import Job
from rq.registry import ScheduledJobRegistry
from rq.scheduler import RQScheduler
from rq.utils import current_timestamp

from .fixtures import say_hello
from tests import RQTestCase


class TestScheduler(RQTestCase):

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
        queue = Queue(connection=self.testconn)
        registry = ScheduledJobRegistry(queue=queue)
        job = Job.create(
            'myfunc',
            args=[12, "☃"],
            kwargs=dict(snowman="☃", null=None),
            connection=self.testconn,
        )
        job.save()
        registry.add(job, 0)
        print('JOB_IDS', registry.get_expired_job_ids())
        scheduler = RQScheduler([queue], connection=self.testconn)
        scheduler.enqueue_scheduled_jobs()
        self.assertEqual(len(queue), 1)

        # After job is scheduled, registry should be empty
        self.assertEqual(len(registry), 0)
