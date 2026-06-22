import sys
import traceback
from unittest import mock

from rq.job import JobStatus
from rq.job_lifecycle import call_exception_handlers, format_exc_info, record_job_failure
from rq.queue import Queue
from rq.registry import FailedJobRegistry
from tests import RQTestCase
from tests.fixtures import say_hello


class JobLifecycleTestCase(RQTestCase):
    def test_chain_continues_unless_handler_returns_falsy(self):
        """None/truthy continue the chain; an explicit falsy return stops it."""
        job = object()

        # None return continues to the next handler
        first_mock = mock.MagicMock(return_value=None)
        second_mock = mock.MagicMock(return_value=None)
        call_exception_handlers([first_mock, second_mock], job, 'exc')
        first_mock.assert_called_once_with(job, 'exc')
        second_mock.assert_called_once_with(job, 'exc')

        # Truthy return continues to the next handler
        first_mock = mock.MagicMock(return_value=True)
        second_mock = mock.MagicMock(return_value=True)
        call_exception_handlers([first_mock, second_mock], job, 'exc')
        first_mock.assert_called_once_with(job, 'exc')
        second_mock.assert_called_once_with(job, 'exc')

        # An explicit falsy return stops the remaining handlers
        first_mock = mock.MagicMock(return_value=False)
        second_mock = mock.MagicMock()
        call_exception_handlers([first_mock, second_mock], job, 'exc')
        first_mock.assert_called_once_with(job, 'exc')
        second_mock.assert_not_called()

    def test_format_exc_info(self):
        """Formats an exc_info tuple exactly like ''.join(traceback.format_exception(...))."""
        try:
            raise ValueError('boom')
        except ValueError:
            exc_info = sys.exc_info()
            self.assertEqual(format_exc_info(exc_info), ''.join(traceback.format_exception(*exc_info)))
            self.assertIn('ValueError: boom', format_exc_info(exc_info))

    def test_record_job_failure(self):
        """record_job_failure sets FAILED status and persists the failure."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        with self.connection.pipeline() as pipeline:
            record_job_failure(job, 'boom traceback', pipeline)
            pipeline.execute()

        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertIn(job.id, FailedJobRegistry(connection=self.connection))
        self.assertIn('boom traceback', job.latest_result().exc_string)
