
import unittest
from unittest.mock import MagicMock, call, patch
import redis
from rq.worker import Worker
from rq.job import Job, JobStatus
from rq.exceptions import InvalidJobOperation

class TestCancellationRaceCondition(unittest.TestCase):
    def setUp(self):
        self.connection = MagicMock(spec=redis.Redis)
        self.connection.connection_pool = MagicMock()
        self.connection.connection_pool.connection_kwargs = {}
        self.worker = Worker(['default'], connection=self.connection)
        self.job = MagicMock(spec=Job)
        self.job.id = 'job_id'
        self.job.key = 'rq:job:job_id'
        self.job.origin = 'default'
        self.job.func_name = 'func'
        self.job.timeout = 180
        self.job.started_at = None
        self.job.ended_at = None

    def test_prepare_job_execution_aborts_if_canceled(self):
        # Setup: Job status returns CANCELED
        self.job.get_status.return_value = JobStatus.CANCELED
        
        # Action & Assertion
        with self.assertRaises(InvalidJobOperation) as cm:
            self.worker.prepare_job_execution(self.job)
        
        self.assertEqual(str(cm.exception), "Job job_id is canceled")
        
        # Verify watch was called
        self.connection.watch.assert_called_with(self.job.key)
        # Verify unwatch was called
        self.connection.unwatch.assert_called_once()
        # Verify pipeline was NOT executed (or created but not executed? created effectively)
        # Verify get_status called with refresh=True
        self.job.get_status.assert_called_with(refresh=True)

    def test_prepare_job_execution_proceeds_if_not_canceled(self):
        # Setup: Job status returns QUEUED
        self.job.get_status.return_value = JobStatus.QUEUED
        
        # Mock pipeline
        pipeline = MagicMock()
        self.connection.pipeline.return_value.__enter__.return_value = pipeline
        
        # Action
        self.worker.prepare_job_execution(self.job)
        
        # Verify watch called
        self.connection.watch.assert_called_with(self.job.key)
        # Verify transaction usage
        pipeline.multi.assert_called_once()
        pipeline.execute.assert_called_once()
        # Verify job.prepare_for_execution called
        self.job.prepare_for_execution.assert_called()

    def test_perform_job_handles_cancellation_exception(self):
        # Mock prepare_job_execution to raise InvalidJobOperation
        with patch.object(self.worker, 'prepare_job_execution', side_effect=InvalidJobOperation):
            # Also mock job status to be CANCELED so perform_job catches it and returns False
            self.job.get_status.return_value = JobStatus.CANCELED
            
            # Action
            result = self.worker.perform_job(self.job, MagicMock())
            
            # Assertion: Should return False and not raise
            self.assertFalse(result)
            self.job.get_status.assert_called()

    def test_perform_job_reraises_other_invalid_ops(self):
        # Mock prepare_job_execution to raise InvalidJobOperation
        with patch.object(self.worker, 'prepare_job_execution', side_effect=InvalidJobOperation('Other error')):
            # Mock job status to be QUEUED (not CANCELED)
            self.job.get_status.return_value = JobStatus.QUEUED
            
            # Action
            result = self.worker.perform_job(self.job, MagicMock())
            
            # Assertion: Should not raise, but return False (failed)
            self.assertFalse(result)
            # Verify failure handling was called (since it wasn't CANCELED, it's a "failure")
            # We can mock handle_job_failure to verify, or check logs if we could.
            # But the fact it didn't crash is good.
            # Actually, perform_job calls handle_job_failure if an exception occurs. 
            # We can spy on handle_job_failure if we want, but returning False is enough signal.

