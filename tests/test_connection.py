from redis import ConnectionPool, Redis, SSLConnection, UnixDomainSocketConnection

from rq import Connection, Queue
from rq.connections import parse_connection
from tests import RQTestCase, find_empty_redis_database
from tests.fixtures import do_nothing


def new_connection():
    return find_empty_redis_database()


class TestConnectionInheritance(RQTestCase):
    def test_connection_detection(self):
        """Automatic detection of the connection."""
        q = Queue()
        self.assertEqual(q.connection, self.testconn)

    def test_connection_stacking(self):
        """Connection stacking."""
        conn1 = Redis(db=4)
        conn2 = Redis(db=5)

        with Connection(conn1):
            q1 = Queue()
            with Connection(conn2):
                q2 = Queue()
        self.assertNotEqual(q1.connection, q2.connection)

    def test_connection_pass_thru(self):
        """Connection passed through from queues to jobs."""
        q1 = Queue(connection=self.testconn)
        with Connection(new_connection()):
            q2 = Queue()
        job1 = q1.enqueue(do_nothing)
        job2 = q2.enqueue(do_nothing)
        self.assertEqual(q1.connection, job1.connection)
        self.assertEqual(q2.connection, job2.connection)

    def test_parse_connection(self):
        """Test parsing the connection"""
        conn_class, pool_class, pool_kwargs = parse_connection(Redis(ssl=True))
        self.assertEqual(conn_class, Redis)
        self.assertEqual(pool_class, SSLConnection)

        path = '/tmp/redis.sock'
        pool = ConnectionPool(connection_class=UnixDomainSocketConnection, path=path)
        conn_class, pool_class, pool_kwargs = parse_connection(Redis(connection_pool=pool))
        self.assertEqual(conn_class, Redis)
        self.assertEqual(pool_class, UnixDomainSocketConnection)
        self.assertEqual(pool_kwargs, {"path": path})
