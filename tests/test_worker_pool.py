import os
import signal
from datetime import datetime, timedelta, timezone
from multiprocessing import Process
from time import sleep

from rq.connections import parse_connection
from rq.job import JobStatus
from rq.queue import Queue
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from rq.worker_pool import WorkerPool, run_worker
from tests import RQTestCase
from tests.fixtures import CustomJob, _send_shutdown_command, long_running_job, say_hello


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

    def test_worker_pool_starts_single_scheduler_non_burst(self):
        """When with_scheduler=True and not burst, pool starts one scheduler process."""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)

        # Stop pool after a short delay
        p = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        p.start()
        pool.start(burst=False, with_scheduler=True)

        # Scheduler should have been created and started at some point
        self.assertIsNotNone(pool.scheduler)
        self.assertIsNotNone(pool.scheduler._process)

    def test_worker_pool_scheduler_burst_no_process(self):
        """In burst mode, pool scheduler enqueues scheduled jobs once and does not start a scheduler process."""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)
        pool.start(burst=True, with_scheduler=True)
        self.assertIsNotNone(pool.scheduler)
        self.assertIsNone(pool.scheduler._process)

    def test_worker_pool_without_scheduler(self):
        """When with_scheduler=False, pool does not create a scheduler."""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)
        p = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 0.5))
        p.start()
        pool.start(burst=False, with_scheduler=False)
        self.assertIsNone(pool.scheduler)

    def test_worker_pool_non_burst_with_scheduler_executes_scheduled_job(self):
        """Non-burst pool with scheduler should execute a job scheduled ~1s in the future."""
        queue = Queue('foo', connection=self.connection)
        # Schedule a job ~1 second in the future
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=1)
        job = queue.enqueue_at(scheduled_time, say_hello)

        pool = WorkerPool(['foo'], connection=self.connection, num_workers=1)

        # Stop pool after a short delay to allow scheduler + worker to process the job
        stopper = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 3.0))
        stopper.start()
        pool.start(burst=False, with_scheduler=True)

        # After pool stops, the scheduled job should have been executed
        self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        # Ensure no lingering workers in case of flakiness
        pool.stop_workers()

    def test_worker_pool_reacquires_scheduler_locks_when_process_missing(self):
        """Pool eventually starts scheduler via acquire_locks(auto_start=True)."""
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)

        # Block initial scheduler lock acquisition so no process is started yet
        self.connection.set('rq:scheduler-lock:default', 'block', ex=1)

        # Allow enough time for the loop to tick and the lock to expire
        stopper = Process(target=wait_and_send_shutdown_signal, args=(os.getpid(), 3.0))
        stopper.start()
        pool.start(burst=False, with_scheduler=True)

        # After running, a scheduler should have been started by the loop
        self.assertIsNotNone(pool.scheduler)
        self.assertIsNotNone(pool.scheduler._process)
        self.assertIsNotNone(getattr(pool.scheduler._process, 'pid', None))
