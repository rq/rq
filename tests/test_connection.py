# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from rq import RQConnection, Queue
from rq.connections import push_connection
from rq.job import Job

from tests import find_empty_redis_database, RQTestCase
from tests.fixtures import do_nothing


class CustomJob(Job):
    pass


class CustomQueue(Queue):
    pass


def new_connection():
    return find_empty_redis_database()


class MockStrictRedis(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class TestConnectionInheritance(RQTestCase):
    def test_connection_requires_parameters(self):
        with self.assertRaises(ValueError):
            RQConnection()

    def test_connection_detection(self):
        """Automatic detection of the connection."""
        push_connection(self.conn)
        q = Queue()
        self.assertEqual(q.connection, self.conn)

    def test_connection_pass_thru(self):
       """Connection passed through from queues to jobs."""
       q1 = Queue(connection=self.conn)
       with RQConnection(new_connection()):
           q2 = Queue()
       job1 = q1.enqueue(do_nothing)
       job2 = q2.enqueue(do_nothing)
       self.assertEqual(q1.connection, job1.connection)
       self.assertEqual(q2.connection, job2.connection)
       self.assertNotEqual(q1.connection, q2.connection)

    def test_redis_conn_pass(self):
        conn = RQConnection(self.testconn)
        self.assertEqual(conn._redis_conn, self.testconn)

    def test_mkqueue_doesnt_write(self):
        conn = RQConnection(self.testconn)
        conn.mkqueue()
        self.assertEqual(self.testconn.keys(), [])

    def test_get_queues(self):
        conn = RQConnection(self.testconn)
        queue1 = conn.mkqueue()
        queue2 = conn.mkqueue()

        # Queues should be creatd yet
        self.assertEqual(conn.get_all_queues(), [])

        queue1.enqueue(do_nothing)
        queue2.enqueue(do_nothing)

        self.assertEqual(set(conn.get_all_queues()),
                         set([queue1, queue2]))

    def test_get_workers(self):
        conn = RQConnection(self.testconn)
        queue = conn.mkqueue()

        # Queues should be creatd yet
        self.assertEqual(conn.get_all_workers(), [])

        worker1 = conn.mkworker(queue, name='w1')
        worker2 = conn.mkworker(queue, name='w2')
        self.assertEqual(conn.get_all_workers(), [])

        worker1.register_birth()
        worker2.register_birth()

        self.assertEqual(set([worker for worker in conn.get_all_workers()]),
                         set([worker1, worker2]))

        self.assertEqual(conn.get_worker('w1'), worker1)
        self.assertEqual(conn.get_worker('w2'), worker2)

    def test_get_job(self):

        conn = RQConnection(self.testconn)
        queue1 = conn.mkqueue('q1')
        queue2 = conn.mkqueue('q2')

        job1 = queue1.enqueue(do_nothing)
        job2 = queue2.enqueue(do_nothing)

        job1_ret = conn.get_job(job1.id)
        job2_ret = conn.get_job(job2.id)

        self.assertEqual(job1_ret.origin, 'q1')
        self.assertEqual(job2_ret.origin, 'q2')
        self.assertEqual(job1_ret.id, job1.id)
        self.assertEqual(job2_ret.id, job2.id)

    def test_custom_job_class(self):
        """Ensure Worker accepts custom job class."""
        conn = RQConnection(self.testconn, job_class=CustomJob)
        j1 = conn.mkjob(func=do_nothing)
        j1.save()
        j2 = conn.get_job(j1.id)

        j3 = self.conn.mkjob(func=do_nothing)
        j3.save()
        j4 = self.conn.get_job(j3.id)

        self.assertTrue(isinstance(j1, CustomJob))
        self.assertTrue(isinstance(j2, CustomJob))
        self.assertTrue(isinstance(j3, Job))
        self.assertTrue(isinstance(j4, Job))

    def test_custom_queue_class(self):
        """Ensure Worker accepts custom queue class."""
        conn = RQConnection(self.testconn, queue_class=CustomQueue)
        q1 = conn.mkqueue('queue1')
        q1.enqueue(do_nothing)

        self.assertTrue(isinstance(q1, CustomQueue))

        q2 = self.conn.mkqueue('queue2')
        q2.enqueue(do_nothing)

        queues = conn.get_all_queues()
        self.assertEqual(len(queues), 2)
        self.assertTrue(isinstance(queues[0], CustomQueue))
        self.assertTrue(isinstance(queues[1], CustomQueue))

        queues = self.conn.get_all_queues()
        self.assertEqual(len(queues), 2)
        self.assertTrue(isinstance(queues[0], Queue))
        self.assertTrue(isinstance(queues[1], Queue))
