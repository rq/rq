import unittest
from blinker import signal
from redis import Redis

testconn = Redis()

class RQTestCase(unittest.TestCase):
    def setUp(self):
        super(RQTestCase, self).setUp()
        testconn.flushdb()
        signal('setup').send(self)

    def tearDown(self):
        signal('teardown').send(self)
        testconn.flushdb()
        super(RQTestCase, self).tearDown()


class TestRQ(RQTestCase):
    def test_math(self):
        assert 1 + 2 == 3


if __name__ == '__main__':
    unittest.main()
