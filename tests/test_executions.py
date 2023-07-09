from tests import RQTestCase
from tests.fixtures import say_hello

from rq.queue import Queue
from rq.executions import Execution, ExecutionRegistry
from rq.utils import now


class TestRegistry(RQTestCase):
    def setUp(self):
        super().setUp()

    def test_add_delete_executions(self):
        """Test adding and deleting executions"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        pipeline = self.connection.pipeline()
        execution = Execution.create(job=job, ttl=100, pipeline=pipeline)
        pipeline.execute()
        created_at = execution.created_at
        composite_key = execution.composite_key
        self.assertTrue(execution.composite_key.startswith(job.id))  # Composite key is prefixed by job ID
        self.assertTrue(self.connection.ttl(execution.key) <= 100)

        execution = Execution.fetch(id=execution.id, job_id=job.id, connection=self.connection)
        self.assertEqual(execution.created_at, created_at)
        self.assertEqual(execution.composite_key, composite_key)

        execution.delete(pipeline=pipeline)
        pipeline.execute()

        self.assertFalse(self.connection.exists(execution.key))

    def test_execution_registry(self):
        """Test the ExecutionRegistry class"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        registry = ExecutionRegistry(job_id=job.id, connection=self.connection)

        pipeline = self.connection.pipeline()
        execution = Execution.create(job=job, ttl=100, pipeline=pipeline)
        pipeline.execute()

        self.assertEqual(self.connection.zcard(registry.key), 1)
        # Registry key TTL should be execution TTL + some buffer time (60 at the moment)
        self.assertTrue(158 <= self.connection.ttl(registry.key) <= 160)

        execution.delete(pipeline=pipeline)
        pipeline.execute()
        self.assertEqual(self.connection.zcard(registry.key), 0)