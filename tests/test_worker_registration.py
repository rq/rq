from tests import RQTestCase

from rq import Queue, Worker
from rq.worker_registration import (get_keys, register, unregister,
                                    WORKERS_BY_QUEUE_KEY)


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
            redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key)
        )
        self.assertTrue(
            redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key)
        )

        unregister(worker)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(
            redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key)
        )
        self.assertFalse(
            redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key)
        )

    def test_get_keys_by_queue(self):
        """get_keys_by_queue only returns active workers for that queue"""
        foo_queue = Queue(name='foo')
        bar_queue = Queue(name='bar')
        baz_queue = Queue(name='baz')

        worker1 = Worker([foo_queue, bar_queue])
        worker2 = Worker([foo_queue])
        worker3 = Worker([baz_queue])

        self.assertEqual(set(), get_keys(foo_queue))

        register(worker1)
        register(worker2)
        register(worker3)

        # get_keys(queue) will return worker keys for that queue
        self.assertEqual(
            set([worker1.key, worker2.key]),
            get_keys(foo_queue)
        )
        self.assertEqual(set([worker1.key]), get_keys(bar_queue))

        # get_keys(connection=connection) will return all worker keys
        self.assertEqual(
            set([worker1.key, worker2.key, worker3.key]),
            get_keys(connection=worker1.connection)
        )

        # Calling get_keys without arguments raises an exception
        self.assertRaises(ValueError, get_keys)

        unregister(worker1)
        unregister(worker2)
        unregister(worker3)
