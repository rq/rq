# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from rq import Connection, Queue

from tests import find_empty_redis_database, RQTestCase
from tests.fixtures import do_nothing


def new_connection():
    return find_empty_redis_database()


class TestConnectionInheritance(RQTestCase):
    def test_connection_detection(self):
        """Automatic detection of the connection."""
        q = Queue()
        self.assertEquals(q.connection, self.testconn)

    def test_connection_stacking(self):
        """Connection stacking."""
        conn1 = new_connection()
        conn2 = new_connection()

        with Connection(conn1):
            q1 = Queue()
            with Connection(conn2):
                q2 = Queue()
        self.assertNotEquals(q1.connection, q2.connection)

    def test_connection_pass_thru(self):
        """Connection passed through from queues to jobs."""
        q1 = Queue()
        with Connection(new_connection()):
            q2 = Queue()
        job1 = q1.enqueue(do_nothing)
        job2 = q2.enqueue(do_nothing)
        self.assertEquals(q1.connection, job1.connection)
        self.assertEquals(q2.connection, job2.connection)
