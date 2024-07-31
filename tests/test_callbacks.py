from datetime import timedelta

from rq import Queue, Worker
from rq.job import UNEVALUATED, Callback, Job, JobStatus
from rq.serializers import JSONSerializer
from rq.worker import SimpleWorker
from tests import RQTestCase
from tests.fixtures import (
    div_by_zero,
    erroneous_callback,
    long_process,
    save_exception,
    save_result,
    save_result_if_not_stopped,
    say_hello,
)


class QueueCallbackTestCase(RQTestCase):
    def test_enqueue_with_success_callback(self):
        """Test enqueue* methods with on_success"""
        queue = Queue(connection=self.connection)

        # Only functions and builtins are supported as callback
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_success=Job.fetch)

        job = queue.enqueue(say_hello, on_success=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_success=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_success=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

    def test_enqueue_with_failure_callback(self):
        """queue.enqueue* methods with on_failure is persisted correctly"""
        queue = Queue(connection=self.connection)

        # Only functions and builtins are supported as callback
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_failure=Job.fetch)

        job = queue.enqueue(say_hello, on_failure=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_failure=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        # test string callbacks
        job = queue.enqueue(say_hello, on_failure=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), say_hello, on_failure=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

    def test_enqueue_with_stopped_callback(self):
        """queue.enqueue* methods with on_stopped is persisted correctly"""
        queue = Queue(connection=self.connection)

        # Only functions and builtins are supported as callback
        with self.assertRaises(ValueError):
            queue.enqueue(say_hello, on_stopped=Job.fetch)

        job = queue.enqueue(long_process, on_stopped=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), long_process, on_stopped=print)

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        # test string callbacks
        job = queue.enqueue(long_process, on_stopped=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

        job = queue.enqueue_in(timedelta(seconds=10), long_process, on_stopped=Callback("print"))

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)

    def test_enqueue_many_callback(self):
        queue = Queue('example', connection=self.connection)

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
        self.assertEqual(self.connection.get('success_callback:%s' % job.id).decode(), job.result)

        job = queue.enqueue(div_by_zero, on_success=save_result)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('success_callback:%s' % job.id))

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback("tests.fixtures.save_result"))
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get('success_callback:%s' % job.id).decode(), job.result)

        job = queue.enqueue(div_by_zero, on_success=Callback("tests.fixtures.save_result"))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('success_callback:%s' % job.id))

    def test_failure_callback(self):
        """queue.enqueue* methods with on_failure is persisted correctly"""
        queue = Queue(is_async=False, connection=self.connection)

        job = queue.enqueue(div_by_zero, on_failure=save_exception)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn('div_by_zero', self.connection.get('failure_callback:%s' % job.id).decode())

        job = queue.enqueue(div_by_zero, on_success=save_result)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('failure_callback:%s' % job.id))

        # test string callbacks
        job = queue.enqueue(div_by_zero, on_failure=Callback("tests.fixtures.save_exception"))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn('div_by_zero', self.connection.get('failure_callback:%s' % job.id).decode())

        job = queue.enqueue(div_by_zero, on_success=Callback("tests.fixtures.save_result"))
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('failure_callback:%s' % job.id))

    def test_stopped_callback(self):
        """queue.enqueue* methods with on_stopped is persisted correctly"""
        connection = self.connection
        queue = Queue('foo', connection=connection, serializer=JSONSerializer)
        worker = SimpleWorker('foo', connection=connection, serializer=JSONSerializer)

        job = queue.enqueue(long_process, on_stopped=save_result_if_not_stopped)
        job.execute_stopped_callback(
            worker.death_penalty_class
        )  # Calling execute_stopped_callback directly for coverage
        self.assertTrue(self.connection.exists('stopped_callback:%s' % job.id))

        # test string callbacks
        job = queue.enqueue(long_process, on_stopped=Callback("tests.fixtures.save_result_if_not_stopped"))
        job.execute_stopped_callback(
            worker.death_penalty_class
        )  # Calling execute_stopped_callback directly for coverage
        self.assertTrue(self.connection.exists('stopped_callback:%s' % job.id))


class WorkerCallbackTestCase(RQTestCase):
    def test_success_callback(self):
        """Test success callback is executed only when job is successful"""
        queue = Queue(connection=self.connection)
        worker = SimpleWorker([queue], connection=self.connection)

        # Callback is executed when job is successfully executed
        job = queue.enqueue(say_hello, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get('success_callback:%s' % job.id).decode(), job.return_value())

        job = queue.enqueue(div_by_zero, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('success_callback:%s' % job.id))

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback("tests.fixtures.save_result"))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(self.connection.get('success_callback:%s' % job.id).decode(), job.return_value())

        job = queue.enqueue(div_by_zero, on_success=Callback("tests.fixtures.save_result"))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('success_callback:%s' % job.id))

    def test_erroneous_success_callback(self):
        """Test exception handling when executing success callback"""
        queue = Queue(connection=self.connection)
        worker = Worker([queue], connection=self.connection)

        # If success_callback raises an error, job will is considered as failed
        job = queue.enqueue(say_hello, on_success=erroneous_callback)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)

        # test string callbacks
        job = queue.enqueue(say_hello, on_success=Callback("tests.fixtures.erroneous_callback"))
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
        self.assertIn('div_by_zero', self.connection.get('failure_callback:%s' % job.id).decode())

        job = queue.enqueue(div_by_zero, on_success=save_result)
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('failure_callback:%s' % job.id))

        # test string callbacks
        job = queue.enqueue(div_by_zero, on_failure=Callback("tests.fixtures.save_exception"))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        job.refresh()
        print(job.exc_info)
        self.assertIn('div_by_zero', self.connection.get('failure_callback:%s' % job.id).decode())

        job = queue.enqueue(div_by_zero, on_success=Callback("tests.fixtures.save_result"))
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.connection.exists('failure_callback:%s' % job.id))

        # TODO: add test case for error while executing failure callback


class JobCallbackTestCase(RQTestCase):
    def test_job_creation_with_success_callback(self):
        """Ensure callbacks are created and persisted properly"""
        job = Job.create(say_hello, connection=self.connection)
        self.assertIsNone(job._success_callback_name)
        # _success_callback starts with UNEVALUATED
        self.assertEqual(job._success_callback, UNEVALUATED)
        self.assertEqual(job.success_callback, None)
        # _success_callback becomes `None` after `job.success_callback` is called if there's no success callback
        self.assertEqual(job._success_callback, None)

        # job.success_callback is assigned properly
        job = Job.create(say_hello, on_success=print, connection=self.connection)
        self.assertIsNotNone(job._success_callback_name)
        self.assertEqual(job.success_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.success_callback, print)

        # test string callbacks
        job = Job.create(say_hello, on_success=Callback("print"), connection=self.connection)
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
        # _failure_callback becomes `None` after `job.failure_callback` is called if there's no failure callback
        self.assertEqual(job._failure_callback, None)

        # job.failure_callback is assigned properly
        job = Job.create(say_hello, on_failure=print, connection=self.connection)
        self.assertIsNotNone(job._failure_callback_name)
        self.assertEqual(job.failure_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.failure_callback, print)

        # test string callbacks
        job = Job.create(say_hello, on_failure=Callback("print"), connection=self.connection)
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
        job = Job.create(say_hello, on_stopped=Callback("print"), connection=self.connection)
        self.assertIsNotNone(job._stopped_callback_name)
        self.assertEqual(job.stopped_callback, print)
        job.save()

        job = Job.fetch(id=job.id, connection=self.connection)
        self.assertEqual(job.stopped_callback, print)
