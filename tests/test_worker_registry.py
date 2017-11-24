from tests import RQTestCase

from rq import Queue, Worker
from rq.worker_registration import register, unregister, workers_by_queue_key


class TestWorkerRegistry(RQTestCase):

    def test_worker_registration(self):
        """Ensure worker.key is correctly set in Redis."""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        worker = Worker([foo_queue, bar_queue])

        register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertTrue(
            redis.sismember(workers_by_queue_key % foo_queue.name, worker.key)
        )
        self.assertTrue(
            redis.sismember(workers_by_queue_key % bar_queue.name, worker.key)
        )

        unregister(worker)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(
            redis.sismember(workers_by_queue_key % foo_queue.name, worker.key)
        )
        self.assertFalse(
            redis.sismember(workers_by_queue_key % bar_queue.name, worker.key)
        )
