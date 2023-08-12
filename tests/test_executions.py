from tests import RQTestCase
from tests.fixtures import say_hello

from rq.queue import Queue
from rq.worker import Worker
from rq.executions import Execution, ExecutionRegistry
from rq.utils import current_timestamp, now


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
        self.assertEqual(execution.last_heartbeat, created_at)

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
    
    def test_ttl(self):
        """Execution registry and job execution should follow heartbeat TTL"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, timeout=-1)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_job_execution(job=job)
        self.assertTrue(self.connection.ttl(job.execution_registry.key) >= worker.get_heartbeat_ttl(job))
        self.assertTrue(self.connection.ttl(worker.execution.key) >= worker.get_heartbeat_ttl(job))

    def test_heartbeat(self):
        """Test heartbeat should refresh execution as well as registry TTL"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, timeout=1)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_job_execution(job=job)

        # The actual TTL should be 150 seconds
        self.assertTrue(1 < self.connection.ttl(job.execution_registry.key) < 160)
        self.assertTrue(1 < self.connection.ttl(worker.execution.key) < 160)
        with self.connection.pipeline() as pipeline:
            worker.execution.heartbeat(job.started_job_registry, 200, pipeline)  # type: ignore
            pipeline.execute()

        # The actual TTL should be 260 seconds for registry and 200 seconds for execution
        self.assertTrue(200 <= self.connection.ttl(job.execution_registry.key) <= 260)
        self.assertTrue(200 <= self.connection.ttl(worker.execution.key) < 260)

    def test_registry_cleanup(self):
        """ExecutionRegistry.cleanup() should remove expired executions."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_job_execution(job=job)

        registry = job.execution_registry
        registry.cleanup()

        self.assertEqual(len(registry), 1)

        registry.cleanup(current_timestamp() + 100)
        self.assertEqual(len(registry), 1)

        # If we pass in a timestamp past execution's TTL, it should be removed.
        # Expiration should be about 150 seconds (worker.get_heartbeat_ttl(job) + 60)
        registry.cleanup(current_timestamp() + 200)
        self.assertEqual(len(registry), 0)

    def test_get_execution_ids(self):
        """ExecutionRegistry.get_execution_ids() should return a list of execution IDs"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_job_execution(job=job)
        execution = worker.execution

        worker.prepare_job_execution(job=job)
        execution_2 = worker.execution

        registry = job.execution_registry
        self.assertEqual(set(registry.get_execution_ids()), {execution.id, execution_2.id})