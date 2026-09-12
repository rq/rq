from datetime import timedelta
from functools import partial
from unittest import mock

from rq import Queue, Worker
from rq.callbacks import execute_failure_callback, execute_stopped_callback, execute_success_callback
from rq.exceptions import DeserializationError
from rq.job import UNEVALUATED, Callback, Job, JobStatus
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from tests import RQTestCase
from tests.fixtures import (
    async_failure_callback,
    async_stopped_callback,
    async_success_callback,
    div_by_zero,
    erroneous_callback,
    long_process,
    save_exception,
    save_result,
    save_result_if_not_stopped,
    save_status_on_failure,
    save_status_on_success,
    say_hello,
)


class CallbackRecorder:
    def __init__(self, value):
        self.value = value

    def __bool__(self):
        return False

    def record(self, job, connection, *args):
        job.meta['callback'] = (self.value, args)
        connection.set(f'callback:{job.id}', self.value)

    def __call__(self, job, connection, *args):
        self.record(job, connection, *args)

    @classmethod
    def record_class(cls, job, connection, *args):
        job.meta['callback'] = (cls.__name__, args)


class InheritedCallbackRecorder(CallbackRecorder):
    pass


class AsyncCallbackRecorder:
    async def __call__(self, job, connection, *args):
        raise AssertionError('Async callbacks must not be invoked')


class CallbackInstanceTestCase(RQTestCase):
    def _assert_callback_roundtrip(self, callback, expected):
        cases = (
            ('success', execute_success_callback, ('result',)),
            ('failure', execute_failure_callback, (ValueError, ValueError('error'), None)),
            ('stopped', execute_stopped_callback, ()),
        )
        for kind, execute, args in cases:
            with self.subTest(kind=kind):
                job = Job.create(
                    say_hello, connection=self.connection, **{f'on_{kind}': Callback(callback, timeout=17)}
                )
                job.save()
                job = Job.fetch(job.id, connection=self.connection)
                execute(job, SimpleWorker.death_penalty_class, *args)
                self.assertEqual(job.meta['callback'], (expected, args))
                self.assertEqual(getattr(job, f'{kind}_callback_timeout'), 17)

    def test_class_method_callbacks(self):
        self._assert_callback_roundtrip(InheritedCallbackRecorder.record_class, 'InheritedCallbackRecorder')

    def test_bound_method_callbacks(self):
        self._assert_callback_roundtrip(CallbackRecorder('method').record, 'method')

    def test_callable_instance_callbacks(self):
        self._assert_callback_roundtrip(CallbackRecorder('instance'), 'instance')

    def test_falsey_callable_instance_callbacks(self):
        cases = (
            ('success', execute_success_callback, ('result',)),
            ('failure', execute_failure_callback, (ValueError, ValueError('error'), None)),
            ('stopped', execute_stopped_callback, ()),
        )
        for kind, execute, args in cases:
            with self.subTest(kind=kind):
                job = Job.create(
                    say_hello,
                    connection=self.connection,
                    **{f'on_{kind}': CallbackRecorder(kind)},
                )
                job.save()
                job = Job.fetch(job.id, connection=self.connection)
                execute(job, SimpleWorker.death_penalty_class, *args)
                self.assertEqual(job.meta['callback'], (kind, args))

    def test_callback_state_is_saved_and_refreshed(self):
        recorder = CallbackRecorder('initial')
        job = Job.create(say_hello, connection=self.connection, on_success=Callback(recorder))
        recorder.value = 'saved'
        job.save()
        restored = Job.fetch(job.id, connection=self.connection)
        execute_success_callback(restored, SimpleWorker.death_penalty_class, 'result')
        self.assertEqual(restored.meta['callback'], ('saved', ('result',)))

        recorder.value = 'resaved'
        job.save()
        restored.refresh()
        execute_success_callback(restored, SimpleWorker.death_penalty_class, 'result')
        self.assertEqual(restored.meta['callback'], ('resaved', ('result',)))

    def test_fetch_and_resave_do_not_deserialize_callbacks(self):
        job = Job.create(say_hello, connection=self.connection, on_success=Callback(CallbackRecorder('pending')))
        job.save()
        with mock.patch.object(job.serializer, 'loads', side_effect=AssertionError('Eager deserialization')) as loads:
            restored = Job.fetch(job.id, connection=self.connection)
            restored.save()
            loads.assert_not_called()

        restored = Job.fetch(job.id, connection=self.connection)
        execute_success_callback(restored, SimpleWorker.death_penalty_class, 'result')
        self.assertEqual(restored.meta['callback'], ('pending', ('result',)))

    def test_callback_uses_job_serializer(self):
        job = Job.create(
            say_hello,
            connection=self.connection,
            serializer=JSONSerializer,
            on_success=Callback(CallbackRecorder('json')),
        )
        with self.assertRaises(TypeError):
            job.to_dict()

    def test_async_callable_callbacks_are_rejected(self):
        cases = (
            ('success', execute_success_callback, (None,)),
            ('failure', execute_failure_callback, (ValueError, ValueError('error'), None)),
            ('stopped', execute_stopped_callback, ()),
        )
        for kind, execute, args in cases:
            with self.subTest(kind=kind):
                job = Job.create(
                    say_hello, connection=self.connection, **{f'on_{kind}': Callback(AsyncCallbackRecorder())}
                )
                job.save()
                job = Job.fetch(job.id, connection=self.connection)
                with self.assertRaises(TypeError):
                    execute(job, SimpleWorker.death_penalty_class, *args)

                job = Job.create(
                    say_hello,
                    connection=self.connection,
                    **{f'on_{kind}': Callback(partial(AsyncCallbackRecorder()))},
                )
                job.save()
                job = Job.fetch(job.id, connection=self.connection)
                with self.assertRaises(TypeError):
                    execute(job, SimpleWorker.death_penalty_class, *args)

        job = Job.create(say_hello, connection=self.connection, on_success=Callback(partial(async_success_callback)))
        with self.assertRaises(TypeError):
            execute_success_callback(job, SimpleWorker.death_penalty_class, None)

    def test_worker_executes_callable_callback(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello, on_success=Callback(CallbackRecorder('worker')))
        SimpleWorker([queue], connection=self.connection).work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get(f'callback:{job.id}'), b'worker')

    def test_callback_deserialization_error(self):
        job = Job.create(say_hello, connection=self.connection, on_success=Callback(CallbackRecorder('broken')))
        job.save()
        job = Job.fetch(job.id, connection=self.connection)
        with mock.patch.object(job.serializer, 'loads', side_effect=ValueError('Invalid callback data')):
            with self.assertRaises(DeserializationError):
                _ = job.success_callback


class QueueCallbackTestCase(RQTestCase):
    def test_enqueue_with_success_callback(self):
        """Test enqueue* methods with on_success"""
        queue = Queue(connection=self.connection)

        # Callback must be a callable or a string
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_success=42)

        job = queue.enqueue(say_hello, on_success=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_success=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_success=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

    def test_enqueue_with_failure_callback(self):
        """queue.enqueue* methods with on_failure is persisted correctly"""
        queue = Queue(connection=self.connection)

        # Callback must be a callable or a string
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_failure=42)

        job = queue.enqueue(say_hello, on_failure=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_failure=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        # test string callbacks
        job = queue.enqueue(say_hello, on_failure=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_failure=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

    def test_enqueue_with_stopped_callback(self):
        """queue.enqueue* methods with on_stopped is persisted correctly"""
        queue = Queue(connection=self.connection)

        # Callback must be a callable or a string
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_stopped=42)

        job = queue.enqueue(long_process, on_stopped=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), long_process, on_stopped=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        # test string callbacks
        job = queue.enqueue(long_process, on_stopped=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), long_process, on_stopped=Callback('print'))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

    def test_enqueue_many_callback(self):
        queue = Queue(connection=self.connection)

        job_data = Queue.prepare_data(
            func=say_hello, on_success=print, on_failure=save_exception, on_stopped=save_result_if_not_stopped
        )

        jobs = queue.enqueue_many([job_data])
        assert jobs[0].success_callback == job_data.on_success
        assert jobs[0].failure_callback == job_data.on_failure
        assert jobs[0].stopped_callback == job_data.on_stopped


class SyncJobCallback(RQTestCase):
    def test_success_callback(self):
        """Test success callback is executed only when job is successful"""
        queue = Queue(is_async=False, connection=self.connection)

        job = queue.enqueue(say_hello, on_success=save_result)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get(f'success_callback:{job.id}').decode(), job.result)

        job = queue.enqueue(div_by_zero, on_success=save_result)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'success_callback:{job.id}'))

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback('tests.fixtures.save_result'))
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get(f'success_callback:{job.id}').decode(), job.result)

        job = queue.enqueue(div_by_zero, on_success=Callback('tests.fixtures.save_result'))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'success_callback:{job.id}'))

    def test_failure_callback(self):
        """queue.enqueue* methods with on_failure is persisted correctly"""
        queue = Queue(is_async=False, connection=self.connection)

        job = queue.enqueue(div_by_zero, on_failure=save_exception)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertTrue(self.connection.exists(f'failure_callback:{job.id}'))

        job = queue.enqueue(div_by_zero, on_success=save_result)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'failure_callback:{job.id}'))

        # test string callbacks
        job = queue.enqueue(div_by_zero, on_failure=Callback('tests.fixtures.save_exception'))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertTrue(self.connection.exists(f'failure_callback:{job.id}'))

        job = queue.enqueue(div_by_zero, on_success=Callback('tests.fixtures.save_result'))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'failure_callback:{job.id}'))

    def test_sync_routes_callbacks_through_execution_helpers(self):
        """Sync execution dispatches callbacks via execute_*_callback (gaining timeout
        wrapping), not by calling the raw callbacks directly."""
        queue = Queue(is_async=False, connection=self.connection)

        with mock.patch('rq.queue.execute_success_callback') as mocked:
            queue.enqueue(say_hello, on_success=save_result)
        mocked.assert_called_once()
        self.assertIs(mocked.call_args.args[1], queue.death_penalty_class)

        with mock.patch('rq.queue.execute_failure_callback') as mocked:
            queue.enqueue(div_by_zero, on_failure=save_exception)
        mocked.assert_called_once()
        self.assertIs(mocked.call_args.args[1], queue.death_penalty_class)

    def test_sync_failure_callback_exception_propagates(self):
        """A raising sync failure callback propagates out, as before the refactor."""
        queue = Queue(is_async=False, connection=self.connection)
        with self.assertRaises(Exception):
            queue.enqueue(div_by_zero, on_failure=erroneous_callback)

    def test_coroutine_callbacks_rejected(self):
        """Coroutine callbacks raise TypeError instead of being called without await."""
        queue = Queue(connection=self.connection)

        job = queue.enqueue(say_hello, on_success=Callback(async_success_callback))
        with self.assertRaises(TypeError):
            execute_success_callback(job, SimpleWorker.death_penalty_class, None)

        job = queue.enqueue(say_hello, on_failure=Callback(async_failure_callback))
        with self.assertRaises(TypeError):
            execute_failure_callback(job, SimpleWorker.death_penalty_class, ValueError, ValueError('x'), None)

        job = queue.enqueue(long_process, on_stopped=Callback(async_stopped_callback))
        with self.assertRaises(TypeError):
            execute_stopped_callback(job, SimpleWorker.death_penalty_class)

    def test_stopped_callback(self):
        """queue.enqueue* methods with on_stopped is persisted correctly"""
        connection = self.connection
        queue = Queue('foo', connection=connection, serializer=JSONSerializer)
        worker = SimpleWorker('foo', connection=connection, serializer=JSONSerializer)

        job = queue.enqueue(long_process, on_stopped=save_result_if_not_stopped)
        execute_stopped_callback(job, worker.death_penalty_class)  # Calling directly for coverage
        self.assertTrue(self.connection.exists(f'stopped_callback:{job.id}'))

        # test string callbacks
        job = queue.enqueue(long_process, on_stopped=Callback('tests.fixtures.save_result_if_not_stopped'))
        execute_stopped_callback(job, worker.death_penalty_class)  # Calling directly for coverage
        self.assertTrue(self.connection.exists(f'stopped_callback:{job.id}'))


class WorkerCallbackTestCase(RQTestCase):
    def test_success_callback(self):
        """Test success callback is executed only when job is successful"""
        queue = Queue(connection=self.connection)
        worker = SimpleWorker([queue], connection=self.connection)

        # Callback is executed when job is successfully executed
        job = queue.enqueue(say_hello, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get(f'success_callback:{job.id}').decode(), job.return_value())

        job = queue.enqueue(div_by_zero, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'success_callback:{job.id}'))

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback('tests.fixtures.save_result'))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get(f'success_callback:{job.id}').decode(), job.return_value())

        job = queue.enqueue(div_by_zero, on_success=Callback('tests.fixtures.save_result'))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'success_callback:{job.id}'))

    def test_erroneous_success_callback(self):
        """Test exception handling when executing success callback"""
        queue = Queue(connection=self.connection)
        worker = Worker([queue], connection=self.connection)

        # If success_callback raises an error, job will is considered as failed
        job = queue.enqueue(say_hello, on_success=erroneous_callback)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback('tests.fixtures.erroneous_callback'))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)

    def test_failure_callback(self):
        """Test failure callback is executed only when job a fails"""
        queue = Queue(connection=self.connection)
        worker = SimpleWorker([queue], connection=self.connection)

        # Callback is executed when job is successfully executed
        job = queue.enqueue(div_by_zero, on_failure=save_exception)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        job.refresh()
        print(job.exc_info)
        self.assertTrue(self.connection.exists(f'failure_callback:{job.id}'))

        job = queue.enqueue(div_by_zero, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'failure_callback:{job.id}'))

        # test string callbacks
        job = queue.enqueue(div_by_zero, on_failure=Callback('tests.fixtures.save_exception'))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        job.refresh()
        print(job.exc_info)
        self.assertTrue(self.connection.exists(f'failure_callback:{job.id}'))

        job = queue.enqueue(div_by_zero, on_success=Callback('tests.fixtures.save_result'))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists(f'failure_callback:{job.id}'))

        # TODO: add test case for error while executing failure callback

    def test_job_status_set_before_success_callback(self):
        """Job status should be FINISHED when success callback runs (#1631)."""
        queue = Queue(connection=self.connection)
        worker = SimpleWorker([queue], connection=self.connection)

        job = queue.enqueue(say_hello, on_success=save_status_on_success)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(
            self.connection.get(f'success_callback_status:{job.id}').decode(),
            JobStatus.FINISHED.value,
        )

    def test_job_status_set_before_failure_callback(self):
        """Job status should be FAILED when failure callback runs (#1631)."""
        queue = Queue(connection=self.connection)
        worker = SimpleWorker([queue], connection=self.connection)

        job = queue.enqueue(div_by_zero, on_failure=save_status_on_failure)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertEqual(
            self.connection.get(f'failure_callback_status:{job.id}').decode(),
            JobStatus.FAILED.value,
        )


class JobCallbackTestCase(RQTestCase):
    def test_job_creation_with_success_callback(self):
        """Ensure callbacks are created and persisted properly"""
        job = Job.create(say_hello, connection=self.connection)
        self.assertIsNone(job._success_callback_name)
        # _success_callback starts with UNEVALUATED
        self.assertEqual(job._success_callback, UNEVALUATED)
        self.assertEqual(job.success_callback, None)

        # job.success_callback is assigned properly
        job = Job.create(say_hello, on_success=print, connection=self.connection)
        self.assertIsNotNone(job._success_callback_name)
        self.assertEqual(job.success_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        # test string callbacks
        job = Job.create(say_hello, on_success=Callback('print'), connection=self.connection)
        self.assertIsNotNone(job._success_callback_name)
        self.assertEqual(job.success_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

    def test_job_creation_with_failure_callback(self):
        """Ensure failure callbacks are persisted properly"""
        job = Job.create(say_hello, connection=self.connection)
        self.assertIsNone(job._failure_callback_name)
        # _failure_callback starts with UNEVALUATED
        self.assertEqual(job._failure_callback, UNEVALUATED)
        self.assertEqual(job.failure_callback, None)

        # job.failure_callback is assigned properly
        job = Job.create(say_hello, on_failure=print, connection=self.connection)
        self.assertIsNotNone(job._failure_callback_name)
        self.assertEqual(job.failure_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        # test string callbacks
        job = Job.create(say_hello, on_failure=Callback('print'), connection=self.connection)
        self.assertIsNotNone(job._failure_callback_name)
        self.assertEqual(job.failure_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

    def test_job_creation_with_stopped_callback(self):
        """Ensure stopped callbacks are persisted properly"""
        job = Job.create(say_hello, connection=self.connection)
        self.assertIsNone(job._stopped_callback_name)
        # _failure_callback starts with UNEVALUATED
        self.assertEqual(job._stopped_callback, UNEVALUATED)
        self.assertEqual(job.stopped_callback, None)
        # _stopped_callback becomes `None` after `job.stopped_callback` is called if there's no stopped callback
        self.assertEqual(job._stopped_callback, None)

        # job.failure_callback is assigned properly
        job = Job.create(say_hello, on_stopped=print, connection=self.connection)
        self.assertIsNotNone(job._stopped_callback_name)
        self.assertEqual(job.stopped_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        # test string callbacks
        job = Job.create(say_hello, on_stopped=Callback('print'), connection=self.connection)
        self.assertIsNotNone(job._stopped_callback_name)
        self.assertEqual(job.stopped_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)
