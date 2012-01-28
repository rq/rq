from tests import RQTestCase
from tests import testjob
from rq import Queue, Worker


class TestWorker(RQTestCase):
    def test_create_worker(self):
        """Worker creation."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.queues, [fooq, barq])

    def test_work_and_quit(self):
        """Worker processes work, then quits."""
        fooq, barq = Queue('foo'), Queue('bar')
        w = Worker([fooq, barq])
        self.assertEquals(w.work(burst=True), False, 'Did not expect any work on the queue.')

        fooq.enqueue(testjob, name='Frank')
        self.assertEquals(w.work(burst=True), True, 'Expected at least some work done.')


