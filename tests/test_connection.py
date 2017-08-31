# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import Connection, Queue, use_connection, get_current_connection, pop_connection
from rq.connections import NoRedisConnectionException

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
        conn1 = new_connection()
        conn2 = new_connection()

        with Connection(conn1):
            q1 = Queue()
            with Connection(conn2):
                q2 = Queue()
        self.assertNotEqual(q1.connection, q2.connection)

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

    def test_resolve_connection_raises_on_no_connection(self):
        """Test function resolve_connection raises if there is no connection."""
        pop_connection()
        with self.assertRaises(NoRedisConnectionException):
            Queue()
