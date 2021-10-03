# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from redis import Redis

from rq import Connection, Queue, use_connection, get_current_connection

from tests import find_empty_redis_database, RQTestCase
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
            q1_conn = Queue().config.connection
            with Connection(conn2):
                q2_conn = Queue().config.connection
        self.assertNotEqual(q1_conn, q2_conn)

    def test_connection_pass_thru(self):
        """Connection passed through from queues to jobs."""
        q1 = Queue()
        with Connection(new_connection()):
            q2 = Queue()
        job1 = q1.enqueue(do_nothing)
        job2 = q2.enqueue(do_nothing)
        self.assertEqual(q1.connection, job1.connection)
        self.assertEqual(q2.connection, job2.connection)


class TestConnectionHelpers(RQTestCase):
    def test_use_connection(self):
        """Test function use_connection works as expected."""
        conn = new_connection()
        use_connection(conn)

        self.assertEqual(conn, get_current_connection())

        use_connection()

        self.assertNotEqual(conn, get_current_connection())

        use_connection(self.testconn)  # Restore RQTestCase connection

        with self.assertRaises(AssertionError):
            with Connection(new_connection()):
                use_connection()
                with Connection(new_connection()):
                    use_connection()
