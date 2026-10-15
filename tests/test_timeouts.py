import time
import unittest
from unittest.mock import Mock, patch

from rq import Queue, SimpleWorker, Worker
from rq.registry import FailedJobRegistry, FinishedJobRegistry
from rq.timeouts import (
    JobTimeoutException,
    TimerDeathPenalty,
    UnixSignalDeathPenalty,
    get_default_death_penalty_class,
)
from tests import RQTestCase


class TestTimeoutExceptionHierarchy(unittest.TestCase):
    @patch('rq.worker.worker_classes.execute_stopped_callback', side_effect=JobTimeoutException('callback timeout'))
    def test_stopped_callback_timeout_still_finalizes_job_failure(self, callback):
        worker = Mock(spec=Worker)
        worker.name = 'test-worker'
        worker.log = Mock()
        worker.death_penalty_class = TimerDeathPenalty
        job = Mock()
        queue = Mock()
        execution = Mock()

        Worker._handle_stopped_job(worker, job, queue, execution)

        callback.assert_called_once_with(job, TimerDeathPenalty)
        worker.handle_job_failure.assert_called_once_with(
            job, queue=queue, exc_string='Job stopped by user, work-horse terminated.', execution=execution
        )

    def test_job_timeout_is_not_caught_by_application_exception_handlers(self):
        def application_job():
            try:
                raise JobTimeoutException('timeout')
            except Exception:
                return 'timeout was swallowed'

        with self.assertRaises(JobTimeoutException):
            application_job()


class TimerBasedWorker(SimpleWorker):
    death_penalty_class = TimerDeathPenalty


def thread_friendly_sleep_func(seconds):
    end_at = time.time() + seconds
    while True:
        if time.time() > end_at:
            break
        time.sleep(0)


class TestTimeouts(RQTestCase):
    def test_timer_death_penalty(self):
        """Ensure TimerDeathPenalty works correctly."""
        q = Queue(connection=self.connection)
        q.empty()
        finished_job_registry = FinishedJobRegistry(connection=self.connection)
        failed_job_registry = FailedJobRegistry(connection=self.connection)

        # make sure death_penalty_class persists
        w = TimerBasedWorker([q], connection=self.connection)
        self.assertIsNotNone(w)
        self.assertEqual(w.death_penalty_class, TimerDeathPenalty)

        # Test short-running job doesn't raise JobTimeoutException
        job = q.enqueue(thread_friendly_sleep_func, args=(1,), job_timeout=3)
        w.work(burst=True)
        job.refresh()
        self.assertIn(job, finished_job_registry)

        # Test long-running job raises JobTimeoutException
        job = q.enqueue(thread_friendly_sleep_func, args=(5,), job_timeout=3)
        w.work(burst=True)
        self.assertIn(job, failed_job_registry)
        job.refresh()
        self.assertIn('rq.timeouts.JobTimeoutException', job.exc_info)

        # Test negative timeout doesn't raise JobTimeoutException,
        # which implies an unintended immediate timeout.
        job = q.enqueue(thread_friendly_sleep_func, args=(1,), job_timeout=-1)
        w.work(burst=True)
        job.refresh()
        self.assertIn(job, finished_job_registry)

    @patch('rq.timeouts.signal')
    def test_get_default_death_penalty_class(self, mock_signal):
        """get_default_death_penalty_class() returns the correct class."""
        # By default, the mock object has a SIGALRM attribute, so
        # get_default_death_penalty_class returns UnixSignalDeathPenalty
        self.assertTrue(hasattr(mock_signal, 'SIGALRM'))
        self.assertEqual(get_default_death_penalty_class(), UnixSignalDeathPenalty)

        # It should return TimerDeathPenalty when SIGALRM is not available
        delattr(mock_signal, 'SIGALRM')
        self.assertFalse(hasattr(mock_signal, 'SIGALRM'))
        self.assertEqual(get_default_death_penalty_class(), TimerDeathPenalty)
