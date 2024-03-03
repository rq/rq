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
        MAX_WORKERS = 41
        MAX_KEYS = 37
        # srem is called twice per invalid key batch: once for WORKERS_BY_QUEUE_KEY; once for REDIS_WORKER_KEYS
        SREM_CALL_COUNT = 2

        queue = Queue(name='foo', connection=self.connection)
        for i in range(MAX_WORKERS):
            worker = Worker([queue], connection=self.connection)
            register(worker)

        with patch('rq.worker_registration.MAX_KEYS', MAX_KEYS), patch.object(
            queue.connection, 'pipeline', wraps=queue.connection.pipeline
        ) as pipeline_mock:
            # clean_worker_registry creates a pipeline with a context manager. Configure the mock using the context
            # manager entry method __enter__
            pipeline_mock.return_value.__enter__.return_value.srem.return_value = None
            pipeline_mock.return_value.__enter__.return_value.execute.return_value = [0] * MAX_WORKERS

            clean_worker_registry(queue)

            expected_call_count = (ceildiv(MAX_WORKERS, MAX_KEYS)) * SREM_CALL_COUNT
            self.assertEqual(pipeline_mock.return_value.__enter__.return_value.srem.call_count, expected_call_count)
