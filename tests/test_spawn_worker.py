import os
import pickle
import shlex
import signal
import sys
import tempfile
import time
from datetime import timezone
from multiprocessing import Process
from pathlib import Path
from unittest import mock

from redis import Redis, SSLConnection, UnixDomainSocketConnection

from rq import Queue
from rq.connections import get_connection_kwargs
from rq.job import Job
from rq.registry import FailedJobRegistry, FinishedJobRegistry
from rq.results import Result
from rq.serializers import JSONSerializer
from rq.worker import SpawnWorker
from tests import RQTestCase, find_empty_redis_database, slow, ssl_test
from tests.fixtures import (
    CustomDeathPenalty,
    CustomJob,
    CustomQueue,
    RecordingSpawnWorker,
    add_meta,
    create_file_after_timeout,
    div_by_zero,
    get_job_id_from_environ,
    kill_worker,
    say_hello,
)


class TestWorker(RQTestCase):
    def test_work_and_quit(self):
        """SpawnWorker processes work, then quits."""
        queue = Queue('foo', connection=self.connection)
        worker = SpawnWorker([queue])
        self.assertEqual(worker.work(burst=True), False, 'Did not expect any work on the queue.')

        job = queue.enqueue(say_hello, name='Frank')
        worker.work(burst=True)

        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(registry.get_job_ids(), [job.id])

        registry = queue.started_job_registry
        self.assertEqual(registry.get_job_ids(), [])

    @ssl_test
    def test_work_with_ssl(self):
        """The work horse connects over SSL when the worker does."""
        connection = find_empty_redis_database(ssl=True)
        queue = Queue('foo', connection=connection)
        job = queue.enqueue(say_hello, name='Frank')
        SpawnWorker([queue], connection=connection).work(burst=True)

        registry = FinishedJobRegistry(queue=queue)
        self.assertEqual(registry.get_job_ids(), [job.id])
        connection.flushdb()

    def test_horse_rebuilds_unix_socket_connection(self):
        connection = Redis(unix_socket_path='/tmp/redis.sock', db=5, password='test-password')
        queue = Queue('foo', connection=connection)
        worker = SpawnWorker([queue], connection=connection, prepare_for_work=False)
        original_kwargs = connection.connection_pool.connection_kwargs.copy()
        worker.execution = mock.Mock(id='execution-id')
        job = Job.create(say_hello, connection=connection)

        with mock.patch('os.posix_spawn', return_value=123) as spawn:
            worker.fork_work_horse(job, queue)

        child_source = spawn.call_args.args[1][2]
        with mock.patch.object(SpawnWorker, 'find_by_key', return_value=None) as find_worker:
            with self.assertRaises(SystemExit) as exited:
                exec(child_source, {})

        self.assertEqual(exited.exception.code, 1)
        child_connection = find_worker.call_args.kwargs['connection']
        self.assertIs(child_connection.connection_pool.connection_class, UnixDomainSocketConnection)
        child_kwargs = child_connection.connection_pool.connection_kwargs
        self.assertEqual(child_kwargs['path'], '/tmp/redis.sock')
        self.assertEqual(child_kwargs['db'], 5)
        self.assertEqual(child_kwargs['password'], 'test-password')
        self.assertEqual(connection.connection_pool.connection_kwargs, original_kwargs)

    def test_horse_rebuilds_ssl_connection(self):
        connection = Redis.from_url('rediss://localhost:6380/5?ssl_cert_reqs=none')
        queue = Queue('foo', connection=connection)
        worker = SpawnWorker([queue], connection=connection, prepare_for_work=False)
        worker.execution = mock.Mock(id='execution-id')
        job = Job.create(say_hello, connection=connection)

        with mock.patch('os.posix_spawn', return_value=123) as spawn:
            worker.fork_work_horse(job, queue)

        child_source = spawn.call_args.args[1][2]
        with mock.patch.object(SpawnWorker, 'find_by_key', return_value=None) as find_worker:
            with self.assertRaises(SystemExit):
                exec(child_source, {})

        child_connection = find_worker.call_args.kwargs['connection']
        self.assertIs(child_connection.connection_pool.connection_class, SSLConnection)
        child_kwargs = child_connection.connection_pool.connection_kwargs
        self.assertEqual(child_kwargs['port'], 6380)
        self.assertEqual(child_kwargs['db'], 5)
        self.assertEqual(child_kwargs['ssl_cert_reqs'], 'none')

    def test_filters_non_serializable_connection_kwargs(self):
        """SpawnWorker strips connection-local runtime objects before rebuilding Redis in the child.

        redis-py 8 adds an unpicklable maintenance-notification handler to connection_kwargs,
        while redis-py 8.1 adds an HImportRegistry; get_connection_kwargs (used by fork_work_horse)
        must drop both so the kwargs can be rebuilt in the spawned process.
        """
        connection = Redis()
        conn_kwargs = connection.connection_pool.connection_kwargs
        conn_kwargs['maint_notifications_pool_handler'] = object()
        conn_kwargs['himport_registry'] = 'runtime state'

        redis_kwargs = get_connection_kwargs(connection)

        self.assertNotIn('maint_notifications_pool_handler', redis_kwargs)
        self.assertNotIn('himport_registry', redis_kwargs)
        self.assertIn('maint_notifications_pool_handler', conn_kwargs)

    def test_worker_normalizes_serializer_arg_for_spawn(self):
        """_serializer_arg is normalized to str|None so it can be safely embedded in the child source."""
        import json

        from rq.serializers import PickleSerializer, resolve_serializer

        queue = Queue('foo', connection=self.connection)

        worker = SpawnWorker([queue])
        self.assertIsNone(worker._serializer_arg)
        self.assertIs(resolve_serializer(worker._serializer_arg), PickleSerializer)

        worker = SpawnWorker([queue], serializer='json')
        self.assertEqual(worker._serializer_arg, 'json')
        self.assertIs(resolve_serializer(worker._serializer_arg), JSONSerializer)

        worker = SpawnWorker([queue], serializer=JSONSerializer)
        self.assertEqual(worker._serializer_arg, 'rq.serializers.JSONSerializer')
        self.assertIs(resolve_serializer(worker._serializer_arg), JSONSerializer)

        worker = SpawnWorker([queue], serializer=json)
        self.assertEqual(worker._serializer_arg, 'json')
        self.assertIs(resolve_serializer(worker._serializer_arg), JSONSerializer)

    def test_invalid_job_id_is_rejected_before_spawn(self):
        queue = Queue('foo', connection=self.connection)

        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, job_id='bad"id')

    def test_work_fails(self):
        """Failing jobs are put on the failed queue."""
        q = Queue(connection=self.connection)
        self.assertEqual(q.count, 0)

        # Action
        job = q.enqueue(div_by_zero)
        self.assertEqual(q.count, 1)

        # keep for later
        enqueued_at_date = job.enqueued_at

        w = SpawnWorker([q])
        w.work(burst=True)

        # Postconditions
        self.assertEqual(q.count, 0)
        failed_job_registry = FailedJobRegistry(queue=q)
        self.assertIn(job, failed_job_registry)
        self.assertEqual(w.get_current_job_id(), None)

        # Check the job
        job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(job.origin, q.name)

        # Should be the original enqueued_at date, not the date of enqueueing
        # to the failed queue
        self.assertEqual(job.enqueued_at.replace(tzinfo=timezone.utc).timestamp(), enqueued_at_date.timestamp())
        result = Result.fetch_latest(job)
        self.assertTrue(result.exc_string)
        self.assertEqual(result.type, Result.Type.FAILED)

    def test_horse_receives_worker_configuration(self):
        """The spawned horse runs with the parent's worker, job, queue and death penalty classes and settings."""
        queue = CustomQueue('foo', connection=self.connection, job_class=CustomJob)
        worker = RecordingSpawnWorker([queue], job_class=CustomJob, queue_class=CustomQueue, default_result_ttl=10000)
        worker.death_penalty_class = CustomDeathPenalty
        job = queue.enqueue(get_job_id_from_environ)

        worker.work(burst=True)

        self.assertEqual(
            self.connection.get('spawn-worker-config'), b'RecordingSpawnWorker CustomJob CustomQueue CustomDeathPenalty'
        )
        self.assertEqual(job.return_value(), job.id)
        self.assertTrue(9000 < self.connection.ttl(job.key) <= 10000)

    def test_horse_uses_exception_handlers(self):
        """The spawned horse runs the parent's exception handlers and honors disable_default_exception_handler."""
        queue = Queue('foo', connection=self.connection)
        worker = SpawnWorker([queue], exception_handlers=[add_meta], disable_default_exception_handler=True)
        job = queue.enqueue(div_by_zero, 1)

        worker.work(burst=True)

        job.refresh()
        self.assertEqual(job.meta, {'foo': 1})
        self.assertNotIn(job, FailedJobRegistry(queue=queue))

    def test_rejects_unpicklable_config_before_dequeue(self):
        queue = Queue('foo', connection=self.connection)
        queue.enqueue(say_hello)
        worker = SpawnWorker([queue], exception_handlers=[lambda job, *exc_info: True])

        with self.assertRaises((pickle.PicklingError, AttributeError)):
            worker.work(burst=True)
        self.assertEqual(queue.count, 1)

    def test_rejects_main_module_config_before_dequeue(self):
        def handler(job, *exc_info):
            return True

        handler.__module__ = '__main__'
        queue = Queue('foo', connection=self.connection)
        queue.enqueue(say_hello)
        worker = SpawnWorker([queue], exception_handlers=[handler])

        with self.assertRaises(ValueError):
            worker.work(burst=True)
        self.assertEqual(queue.count, 1)

    def test_horse_gets_own_process_group_at_spawn(self):
        """The horse is in its own process group before any of its code runs."""
        queue = Queue('foo', connection=self.connection)
        queue.enqueue(say_hello)
        worker = SpawnWorker([queue])

        with tempfile.TemporaryDirectory() as directory:
            # Holds the child before the interpreter starts, so the group must already be set at spawn
            release_file = Path(directory) / 'release'
            wrapper_path = Path(directory) / 'python'
            wrapper_path.write_text(
                '#!/bin/sh\n'
                'i=0\n'
                f'while [ ! -e {shlex.quote(str(release_file))} ]; do\n'
                '    i=$((i + 1))\n'
                '    [ "$i" -gt 1000 ] && exit 1\n'
                '    sleep 0.01\n'
                'done\n'
                f'exec {shlex.quote(sys.executable)} "$@"\n'
            )
            wrapper_path.chmod(0o755)

            worker.register_birth()
            try:
                job, queue = worker.dequeue_job_and_maintain_ttl(5)
                execution = worker.prepare_execution(job)
                with mock.patch('sys.executable', str(wrapper_path)):
                    worker.fork_work_horse(job, queue)
                try:
                    self.assertEqual(os.getpgid(worker.horse_pid), worker.horse_pid)
                finally:
                    release_file.touch()
                    worker.monitor_work_horse(job, queue, execution)
            finally:
                worker.register_death()


def wait_and_kill_work_horse(pid, time_to_wait=0.0):
    time.sleep(time_to_wait)
    os.kill(pid, signal.SIGKILL)


class TimeoutTestCase:
    def setUp(self):
        # we want tests to fail if signal are ignored and the work remain
        # running, so set a signal to kill them after X seconds
        self.killtimeout = 15
        signal.signal(signal.SIGALRM, self._timeout)
        signal.alarm(self.killtimeout)

    def _timeout(self, signal, frame):
        raise AssertionError(
            f"test still running after {self.killtimeout} seconds, likely the worker wasn't shutdown correctly"
        )


class WorkerShutdownTestCase(TimeoutTestCase, RQTestCase):
    @slow
    def test_idle_worker_warm_shutdown(self):
        """worker with no ongoing job receiving single SIGTERM signal and shutting down"""
        w = SpawnWorker('foo', connection=self.connection)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), False))
        p.start()

        w.work()

        p.join(1)
        self.assertFalse(w._stop_requested)

    @slow
    def test_working_worker_cold_shutdown(self):
        """Busy worker shuts down immediately on double SIGTERM signal"""
        fooq = Queue('foo', connection=self.connection)
        w = SpawnWorker(fooq)

        sentinel_file = '/tmp/.rq_sentinel_cold'
        self.assertFalse(
            os.path.exists(sentinel_file), f'{sentinel_file} file should not exist yet, delete that file and try again.'
        )
        fooq.enqueue(create_file_after_timeout, sentinel_file, 5)
        self.assertFalse(w._stop_requested)
        p = Process(target=kill_worker, args=(os.getpid(), True))
        p.start()

        self.assertRaises(SystemExit, w.work)

        p.join(1)
        self.assertTrue(w._stop_requested)
        self.assertFalse(os.path.exists(sentinel_file))

        shutdown_requested_date = w.shutdown_requested_date
        self.assertIsNotNone(shutdown_requested_date)
        self.assertEqual(type(shutdown_requested_date).__name__, 'datetime')
