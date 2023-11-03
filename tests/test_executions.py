from time import sleep

from rq.executions import Execution, ExecutionRegistry
from rq.queue import Queue
from rq.utils import current_timestamp, utcnow
from rq.worker import Worker
from tests import RQTestCase
from tests.fixtures import long_running_job, say_hello, start_worker_process


class TestRegistry(RQTestCase):
    def setUp(self):
        super().setUp()

    def test_equality(self):
        """Test equality between Execution objects"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        pipeline = self.connection.pipeline()
        execution_1 = Execution.create(job=job, ttl=100, pipeline=pipeline)
        execution_2 = Execution.create(job=job, ttl=100, pipeline=pipeline)
        pipeline.execute()
        self.assertNotEqual(execution_1, execution_2)
        fetched_execution = Execution.fetch(id=execution_1.id, job_id=job.id, connection=self.connection)
        self.assertEqual(execution_1, fetched_execution)

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

        execution.delete(job=job, pipeline=pipeline)
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

        execution.delete(pipeline=pipeline, job=job)
        pipeline.execute()
        self.assertEqual(self.connection.zcard(registry.key), 0)

    def test_ttl(self):
        """Execution registry and job execution should follow heartbeat TTL"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, timeout=-1)
        worker = Worker([queue], connection=self.connection)
        execution = worker.prepare_execution(job=job)
        self.assertTrue(self.connection.ttl(job.execution_registry.key) >= worker.get_heartbeat_ttl(job))
        self.assertTrue(self.connection.ttl(execution.key) >= worker.get_heartbeat_ttl(job))

    def test_heartbeat(self):
        """Test heartbeat should refresh execution as well as registry TTL"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, timeout=1)
        worker = Worker([queue], connection=self.connection)
        execution = worker.prepare_execution(job=job)

        # The actual TTL should be 150 seconds
        self.assertTrue(1 < self.connection.ttl(job.execution_registry.key) < 160)
        self.assertTrue(1 < self.connection.ttl(execution.key) < 160)
        with self.connection.pipeline() as pipeline:
            worker.execution.heartbeat(job.started_job_registry, 200, pipeline)  # type: ignore
            pipeline.execute()

        # The actual TTL should be 260 seconds for registry and 200 seconds for execution
        self.assertTrue(200 <= self.connection.ttl(job.execution_registry.key) <= 260)
        self.assertTrue(200 <= self.connection.ttl(execution.key) < 260)

    def test_registry_cleanup(self):
        """ExecutionRegistry.cleanup() should remove expired executions."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.prepare_execution(job=job)

        registry = job.execution_registry
        registry.cleanup()

        self.assertEqual(len(registry), 1)

        registry.cleanup(current_timestamp() + 100)
        self.assertEqual(len(registry), 1)

        # If we pass in a timestamp past execution's TTL, it should be removed.
        # Expiration should be about 150 seconds (worker.get_heartbeat_ttl(job) + 60)
        registry.cleanup(current_timestamp() + 200)
        self.assertEqual(len(registry), 0)

    def test_delete_registry(self):
        """ExecutionRegistry.delete() should delete registry and its executions."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        execution = worker.prepare_execution(job=job)

        self.assertIn(execution.composite_key, job.started_job_registry.get_job_ids())

        registry = job.execution_registry
        pipeline = self.connection.pipeline()
        registry.delete(job=job, pipeline=pipeline)
        pipeline.execute()

        self.assertNotIn(execution.composite_key, job.started_job_registry.get_job_ids())
        self.assertFalse(self.connection.exists(registry.key))

    def test_get_execution_ids(self):
        """ExecutionRegistry.get_execution_ids() should return a list of execution IDs"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)

        execution = worker.prepare_execution(job=job)
        execution_2 = worker.prepare_execution(job=job)

        registry = job.execution_registry
        self.assertEqual(set(registry.get_execution_ids()), {execution.id, execution_2.id})

    def test_execution_added_to_started_job_registry(self):
        """Ensure worker adds execution to started job registry"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(long_running_job, timeout=3)
        Worker([queue], connection=self.connection)

        # Start worker process in background with 1 second monitoring interval
        process = start_worker_process(
            queue.name, worker_name='w1', connection=self.connection, burst=True, job_monitoring_interval=1
        )

        sleep(0.5)
        # Execution should be registered in started job registry
        execution = job.get_executions()[0]
        self.assertEqual(len(job.get_executions()), 1)
        self.assertIn(execution.composite_key, job.started_job_registry.get_job_ids())

        last_heartbeat = execution.last_heartbeat
        last_heartbeat = utcnow()
        self.assertTrue(30 < self.connection.ttl(execution.key) < 200)

        sleep(2)
        # During execution, heartbeat should be updated, this test is flaky on MacOS
        execution.refresh()
        self.assertNotEqual(execution.last_heartbeat, last_heartbeat)
        process.join(10)

        # When job is done, execution should be removed from started job registry
        self.assertNotIn(execution.composite_key, job.started_job_registry.get_job_ids())
        self.assertEqual(job.get_status(), 'finished')
