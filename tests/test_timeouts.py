from rq import Queue, SimpleWorker
from rq.timeouts import JobTimeoutException, TimerDeathPenalty
from tests import RQTestCase
from tests.fixtures import long_running_job, say_hello


class TimerBasedWorker(SimpleWorker):
    death_penalty_class = TimerDeathPenalty


class TestTimeouts(RQTestCase):
    def test_timer_death_penalty(self):
        """Ensure TimerDeathPenalty works correctly."""
        q = Queue('foo')
        # make sure death_penalty_class persists
        w = TimerBasedWorker(q, timeout=3)
        self.assertIsNotNone(w)
        self.assertEqual(w.death_penalty_class, TimerDeathPenalty)

        # Test short-running job doesnt raise JobTimeoutException
        job = q.enqueue(say_hello, name='Mike')
        w.work(burst=True)
        self.assertIn(job, job.finished_job_registry)
        job.refresh()
        self.assertNotIn('rq.timeouts.JobTimeoutException', job.exc_info)
        
        # Test long-running job raises JobTimeoutException
        job = q.enqueue(long_running_job)
        w.work(burst=True)
        self.assertIn(job, job.failed_job_registry)
        job.refresh()
        self.assertIn('rq.timeouts.JobTimeoutException', job.exc_info)
