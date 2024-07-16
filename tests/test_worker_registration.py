from unittest.mock import patch

from rq import Queue, Worker
from rq.utils import ceildiv
from rq.worker_registration import (
    REDIS_WORKER_KEYS,
    WORKERS_BY_QUEUE_KEY,
    clean_worker_registry,
    get_keys,
    register,
    unregister,
)
from tests import RQTestCase


class TestWorkerRegistry(RQTestCase):
    def test_worker_registration(self):
        """Ensure worker.key is correctly set in Redis."""
        foo_queue = Queue(name='foo', connection=self.connection)
        bar_queue = Queue(name='bar', connection=self.connection)
        worker = Worker([foo_queue, bar_queue], connection=self.connection)

        register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertEqual(Worker.count(connection=redis), 1)
        self.assertTrue(redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key))
        self.assertEqual(Worker.count(queue=foo_queue), 1)
        self.assertTrue(redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key))
        self.assertEqual(Worker.count(queue=bar_queue), 1)

        unregister(worker)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(redis.sismember(WORKERS_BY_QUEUE_KEY % foo_queue.name, worker.key))
        self.assertFalse(redis.sismember(WORKERS_BY_QUEUE_KEY % bar_queue.name, worker.key))

    def test_get_keys_by_queue(self):
        """get_keys_by_queue only returns active workers for that queue"""
        foo_queue = Queue(name='foo', connection=self.connection)
        bar_queue = Queue(name='bar', connection=self.connection)
        baz_queue = Queue(name='baz', connection=self.connection)

        worker1 = Worker([foo_queue, bar_queue], connection=self.connection)
        worker2 = Worker([foo_queue], connection=self.connection)
        worker3 = Worker([baz_queue], connection=self.connection)

        self.assertEqual(set(), get_keys(foo_queue))

        register(worker1)
        register(worker2)
        register(worker3)

        # get_keys(queue) will return worker keys for that queue
        self.assertEqual(set([worker1.key, worker2.key]), get_keys(foo_queue))
        self.assertEqual(set([worker1.key]), get_keys(bar_queue))

        # get_keys(connection=connection) will return all worker keys
        self.assertEqual(set([worker1.key, worker2.key, worker3.key]), get_keys(connection=worker1.connection))

        # Calling get_keys without arguments raises an exception
        self.assertRaises(ValueError, get_keys)

        unregister(worker1)
        unregister(worker2)
        unregister(worker3)

    def test_clean_registry(self):
        """clean_registry removes worker keys that don't exist in Redis"""
        queue = Queue(name='foo', connection=self.connection)
        worker = Worker([queue], connection=self.connection)

        register(worker)
        redis = worker.connection

        self.assertTrue(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertTrue(redis.sismember(REDIS_WORKER_KEYS, worker.key))

        clean_worker_registry(queue)
        self.assertFalse(redis.sismember(worker.redis_workers_keys, worker.key))
        self.assertFalse(redis.sismember(REDIS_WORKER_KEYS, worker.key))

    def test_clean_large_registry(self):
        """
        clean_registry() splits invalid_keys into multiple lists for set removal to avoid sending more than redis can
        receive
        """
        worker_count = 11
        MAX_KEYS = 6
        SREM_CALL_COUNT = 2

        queue = Queue(name='foo', connection=self.connection)
        for i in range(worker_count):
            worker = Worker([queue], connection=self.connection)
            register(worker)

        # Since we registered 11 workers and set the maximum keys to be deleted in each command to 6,
        # `srem` command should be called a total of 4 times.
        # `srem` is called twice per invalid key group; once for WORKERS_BY_QUEUE_KEY and once for REDIS_WORKER_KEYS
        with patch('rq.worker_registration.MAX_KEYS', MAX_KEYS), patch('redis.client.Pipeline.srem') as mock:
            clean_worker_registry(queue)
            expected_call_count = (ceildiv(worker_count, MAX_KEYS)) * SREM_CALL_COUNT
            self.assertEqual(mock.call_count, expected_call_count)
