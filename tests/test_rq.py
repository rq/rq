import unittest
from blinker import signal
from redis import Redis
import rq
from rq import Queue

class RQTestCase(unittest.TestCase):
    def setUp(self):
        super(RQTestCase, self).setUp()

        rq.push_connection(Redis())
        self.conn = rq.current_connection()

        self.conn.flushdb()
        signal('setup').send(self)

    def tearDown(self):
        signal('teardown').send(self)

        self.conn.flushdb()
        conn = rq.pop_connection()
        assert conn == self.conn

        super(RQTestCase, self).tearDown()


class TestQueue(RQTestCase):
    def test_create_queue(self):
        """Creating queues."""
        q = Queue('my-queue')
        self.assertEquals(q.name, 'my-queue')

    def test_queue_empty(self):
        """Detecting empty queues."""
        q = Queue('my-queue')
        self.assertEquals(q.empty, True)

        self.conn.rpush('rq:my-queue', 'some val')
        self.assertEquals(q.empty, False)


if __name__ == '__main__':
    unittest.main()
