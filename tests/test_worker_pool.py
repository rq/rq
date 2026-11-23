import logging
import os
import signal
from multiprocessing import Process
from time import sleep
from unittest import mock

from rq.connections import get_connection_kwargs, parse_connection
from rq.job import JobStatus
from rq.queue import Queue
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from rq.worker_pool import WorkerPool, run_worker
from tests import RQTestCase
from tests.fixtures import (
    CustomJob,
    CustomQueue,
    _send_shutdown_command,
    add_meta,
    div_by_zero,
    long_running_job,
    say_hello,
)


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
        _send_shutdown_command(worker_data.name, get_connection_kwargs(self.connection), delay=0)
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
        _send_shutdown_command(worker_data.name, get_connection_kwargs(self.connection), delay=0)
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

    def test_stop_requested_during_worker_start_stops_new_worker(self):
        """A stop that arrives while a worker process is starting must still reach that worker.

        Regression test for https://github.com/rq/rq/issues/2590: start_worker()
        used to register the worker in worker_dict only after process.start()
        returned, so a SIGINT/SIGTERM handled in that window missed the new
        worker and left it running.
        """
        pool = WorkerPool(['default'], connection=self.connection, num_workers=1)

        fake_process = mock.MagicMock()
        fake_process.pid = 4242

        def start_and_request_stop():
            # Simulates the signal handler running while the child is starting
            pool.request_stop()

        fake_process.start.side_effect = start_and_request_stop

        with (
            mock.patch.object(pool, 'get_worker_process', return_value=fake_process),
            mock.patch('rq.worker_pool.os.kill') as mock_kill,
        ):
            pool.start_worker()

        # The worker registered after the stop request must still be signaled
        mock_kill.assert_called_once_with(4242, signal.SIGINT)
        self.assertEqual(pool.status, pool.Status.STOPPED)

    def test_run_worker_resets_inherited_pool_signal_handlers(self):
        """run_worker() must not keep the pool's SIGINT/SIGTERM handlers in the child.

        Regression test for https://github.com/rq/rq/issues/2590: before the
        worker installs its own handlers in work(), an inherited pool handler
        would swallow a stop signal instead of terminating the process.
        """
        seen = {}

        class FakeWorker:
            def __init__(self, *args, **kwargs):
                self.log = logging.getLogger('fake-worker')

            def work(self, **kwargs):
                # work() normally installs the worker's own handlers; a worker
                # that hasn't gotten there yet must see the defaults instead
                seen['sigint'] = signal.getsignal(signal.SIGINT)
                seen['sigterm'] = signal.getsignal(signal.SIGTERM)

        def pool_handler(signum, frame):
            pass

        old_sigint = signal.signal(signal.SIGINT, pool_handler)
        old_sigterm = signal.signal(signal.SIGTERM, pool_handler)
        try:
            run_worker(
                'test-worker',
                ['default'],
                connection_class=lambda connection_pool: object(),
                connection_pool_class=lambda **kwargs: object(),
                connection_pool_kwargs={},
                worker_class=FakeWorker,
                queue_class=lambda name, connection: object(),
            )
        finally:
            signal.signal(signal.SIGINT, old_sigint)
            signal.signal(signal.SIGTERM, old_sigterm)

        self.assertEqual(seen['sigint'], signal.SIG_DFL)
        self.assertEqual(seen['sigterm'], signal.SIG_DFL)

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

        # Clean up
        pool.stop_workers()

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
        try:
            pool.start(burst=True)

            self.assertEqual(job.get_status(refresh=True), JobStatus.FAILED)
            job.refresh()
            self.assertEqual(job.meta, {'foo': 1})
        finally:
            pool.stop_workers()

    def test_queue_class_propagated_to_workers(self):
        """Ensure queue_class is passed to workers spawned by the pool"""
        queue = CustomQueue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)

        pool = WorkerPool([queue], connection=self.connection, num_workers=1, queue_class=CustomQueue)
        try:
            pool.start(burst=True)

            self.assertEqual(job.get_status(refresh=True), JobStatus.FINISHED)
        finally:
            pool.stop_workers()

    def test_queues_use_job_class(self):
        """Ensure WorkerPool.queues are built with the pool's job class"""
        pool = WorkerPool(['foo'], connection=self.connection, job_class=CustomJob)
        self.assertIs(pool.queues[0].job_class, CustomJob)
