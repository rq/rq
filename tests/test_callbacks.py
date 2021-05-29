from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello, save_result

from rq import Queue, Worker
from rq.job import Job, JobStatus


class QueueCallbackTest(RQTestCase):
    def test_enqueue_with_callbacks(self):
        """Test enqueue* methods with callbacks"""
        queue = Queue(connection=self.testconn)
        job = queue.enqueue(say_hello, on_success=print)

        job = Job.fetch(id=job.id, connection=self.testconn)
        self.assertEqual(job.success_callback, print)


class TestWorker(RQTestCase):
    def test_success_callback(self):
        """Test success callback is executed only when job is successful"""
        queue = Queue(connection=self.testconn)
        worker = Worker([queue])

        job = queue.enqueue(say_hello, on_success=save_result)

        # Callback is executed when job is successfully executed
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FINISHED)
        self.assertEqual(
            self.testconn.get('success_callback:%s' % job.id).decode(),
            job.result
        )

        job = queue.enqueue(div_by_zero, on_success=save_result)        
        worker.work(burst=True)
        self.assertEqual(job.get_status(), JobStatus.FAILED)
        self.assertFalse(self.testconn.exists('success_callback:%s' % job.id))
