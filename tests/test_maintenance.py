import unittest
from unittest.mock import patch

from redis import Redis

from rq.job import JobStatus
from rq.maintenance import clean_intermediate_queue
from rq.queue import Queue
from rq.utils import get_version
from rq.worker import Worker
from tests import RQTestCase
from tests.fixtures import say_hello


class MaintenanceTestCase(RQTestCase):
    @unittest.skipIf(get_version(Redis()) < (6, 2, 0), 'Skip if Redis server < 6.2.0')
    def test_cleanup_intermediate_queue(self):
        """Ensure jobs stuck in the intermediate queue are cleaned up."""
        queue = Queue('foo', connection=self.connection)
        job = queue.enqueue(say_hello)

        # If job execution fails after it's dequeued, job should be in the intermediate queue
        # # and it's status is still QUEUED
        with patch.object(Worker, 'execute_job'):
            # mocked.execute_job.side_effect = Exception()
            worker = Worker(queue, connection=self.connection)
            worker.work(burst=True)

            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertFalse(job.id in queue.get_job_ids())
            self.assertIsNotNone(self.connection.lpos(queue.intermediate_queue_key, job.id))
            # After cleaning up the intermediate queue, job status should be `FAILED`
            # and job is also removed from the intermediate queue
            clean_intermediate_queue(worker, queue)
            self.assertEqual(job.get_status(), JobStatus.FAILED)
            self.assertIsNone(self.connection.lpos(queue.intermediate_queue_key, job.id))
