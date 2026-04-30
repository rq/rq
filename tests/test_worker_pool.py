import os
import signal
from multiprocessing import Process
from time import sleep

from rq.connections import parse_connection
from rq.job import JobStatus
from rq.queue import Queue
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from rq.worker_pool import WorkerPool, run_worker
from tests import RQTestCase
from tests.fixtures import CustomJob, _send_shutdown_command, add_meta, div_by_zero, long_running_job, say_hello


def wait_and_send_shutdown_signal(pid, time_to_wait=0.0):
    sleep(time_to_wait)
    os.kill(pid, signal.SIGTERM)


class TestWorkerPool(RQTestCase):
    def test_queues(self):
        """Test queue parsing"""
        pool = WorkerPool(['default', 'foo'], connection=self.connection)
        self.assertEqual(
            set(pool.queues), {Queue('default', connection=self.connection), Queue('foo', connection=self.connection)}
        )

    # def test_spawn_workers(self):
    #     """Test spawning workers"""
    #     pool = WorkerPool(['default', 'foo'], connection=self.connection, num_workers=2)
    #     pool.start_workers(burst=False)
    #     self.assertEqual(len(pool.worker_dict.keys()), 2)
    #     pool.stop_workers()

    def test_check_workers(self):
        """Test check_workers()"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.check_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        sleep(0.5)
        _send_shutdown_command(worker_data.name, self.connection.connection_pool.connection_kwargs.copy(), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.75)
        pool.check_workers(respawn=False)
        self.assertEqual(len(pool.worker_dict.keys()), 1)

        # If we call `check_workers` with `respawn=True`, the worker should be respawned
        pool.check_workers(respawn=True)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        pool.stop_workers()

    def test_reap_workers(self):
        """Dead workers are removed from worker_dict"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        sleep(0.5)
        _send_shutdown_command(worker_data.name, self.connection.connection_pool.connection_kwargs.copy(), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.75)
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 1)
        pool.stop_workers()

    def test_start(self):
        """Test start()"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)

        p = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        p.start()
        pool.start()
        self.assertEqual(pool.status, pool.Status.STOPPED)
        self.assertTrue(pool.all_workers_have_stopped())
        # We need this line so the test doesn't hang
        pool.stop_workers()

    def test_pool_ignores_consecutive_shutdown_signals(self):
        """If two shutdown signals are sent within one second, only the first one is processed"""
        # Send two shutdown signals within one second while the worker is
        # working on a long running job. The job should still complete (not killed)
        pool = WorkerPool(['foo'], connection=self.connection, num_workers=2)

        process_1 = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        process_1.start()
        process_2 = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        process_2.start()

        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(long_running_job, 1)
        pool.start(burst=True)

        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        # We need this line so the test doesn't hang
        pool.stop_workers()

    def test_run_worker(self):
        """Ensure run_worker() properly spawns a Worker"""
        queue = Queue('foo', connection=self.connection)
        queue.enqueue(say_hello)

        connection_class, pool_class, pool_kwargs = parse_connection(self.connection)
        run_worker('test-worker', ['foo'], connection_class, pool_class, pool_kwargs)
        # Worker should have processed the job
        self.assertEqual(len(queue), 0)

    def test_worker_pool_arguments(self):
        """Ensure arguments are properly used to create the right workers"""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)
        pool = WorkerPool([queue], connection=self.connection, num_workers=2, worker_class=SimpleWorker)
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

        queue = Queue('json', connection=self.connection, serializer=JSONSerializer)
        job = queue.enqueue(say_hello, 'Hello')
        pool = WorkerPool(
            [queue], connection=self.connection, num_workers=2, worker_class=SimpleWorker, serializer=JSONSerializer
        )
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

        pool = WorkerPool([queue], connection=self.connection, num_workers=2, job_class=CustomJob)
        pool.start(burst=True)
        # Worker should have processed the job
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)

    def test_exception_handlers_argument(self):
        """Ensure exception_handlers argument is properly passed to WorkerPool"""
        pool = WorkerPool(
            ['default'], connection=self.connection, num_workers=1, exception_handlers=[add_meta]
        )
        self.assertEqual(pool.exception_handlers, [add_meta])

    def test_exception_handlers_with_none(self):
        """Ensure WorkerPool works when exception_handlers is None"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1, exception_handlers=None)
        self.assertIsNone(pool.exception_handlers)

    def test_exception_handlers_propagated_to_workers(self):
        """Ensure exception_handlers are passed to workers spawned by the pool"""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(div_by_zero)

        pool = WorkerPool(
            [queue], connection=self.connection, num_workers=1, exception_handlers=[add_meta]
        )
        pool.start(burst=True)

        # Job should be failed (exception handled by custom handler)
        self.assertEqual(job.get_status(refresh=True), JobStatus.FAILED)
        job.refresh()
        # Custom handler should have been called (add_meta sets foo=1)
        self.assertEqual(job.meta, {'foo': 1})

    def test_request_stop(self):
        """Test request_stop() changes status to STOPPED"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)
        pool.start_workers(burst=False)

        # Verify workers are running
        self.assertEqual(len(pool.worker_dict), 1)

        # Call request_stop
        pool.request_stop()

        # Status should be STOPPED
        self.assertEqual(pool.status, pool.Status.STOPPED)

        # Clean up
        pool.stop_workers()

    def test_handle_dead_worker(self):
        """Test handle_dead_worker() removes worker from worker_dict"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        self.assertEqual(len(pool.worker_dict), 2)

        # Get a worker data
        worker_data = list(pool.worker_dict.values())[0]

        # Manually remove the worker from the dict (simulating death)
        pool.handle_dead_worker(worker_data)

        # Worker should be removed
        self.assertEqual(len(pool.worker_dict), 1)

        pool.stop_workers()

    def test_stop_worker(self):
        """Test stop_worker() sends signal to worker"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)
        pool.start_workers(burst=True)

        # There should be one worker
        self.assertEqual(len(pool.worker_dict), 1)

        worker_data = list(pool.worker_dict.values())[0]

        # Stop the worker
        pool.stop_worker(worker_data)

        # Wait for process to terminate properly
        worker_data.process.join(timeout=2.0)

        # Reap workers
        pool.reap_workers()

        # Worker should be removed
        self.assertEqual(len(pool.worker_dict), 0)

    def test_start_worker_with_sleep(self):
        """Test start_worker() with _sleep parameter"""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)

        # Start worker with a small sleep
        pool.start_worker(burst=True, _sleep=0.1)

        # Worker should be spawned
        self.assertEqual(len(pool.worker_dict), 1)

        pool.stop_workers()
