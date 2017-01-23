# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import mock
from redis import StrictRedis
from rq.decorators import job
from rq.job import Job
from rq.worker import DEFAULT_RESULT_TTL
from rq.queue import Queue

from tests import RQTestCase
from tests.fixtures import decorated_job


class TestDecorator(RQTestCase):

    def setUp(self):
        super(TestDecorator, self).setUp()

    def test_decorator_preserves_functionality(self):
        """Ensure that a decorated function's functionality is still preserved.
        """
        self.assertEqual(decorated_job(1, 2), 3)

    def test_decorator_adds_delay_attr(self):
        """Ensure that decorator adds a delay attribute to function that returns
        a Job instance when called.
        """
        self.assertTrue(hasattr(decorated_job, 'delay'))
        result = decorated_job.delay(1, 2)
        self.assertTrue(isinstance(result, Job))
        # Ensure that job returns the right result when performed
        self.assertEqual(result.perform(), 3)

    def test_decorator_accepts_queue_name_as_argument(self):
        """Ensure that passing in queue name to the decorator puts the job in
        the right queue.
        """
        @job(queue='queue_name')
        def hello():
            return 'Hi'
        result = hello.delay()
        self.assertEqual(result.origin, 'queue_name')

    def test_decorator_accepts_result_ttl_as_argument(self):
        """Ensure that passing in result_ttl to the decorator sets the
        result_ttl on the job
        """
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.result_ttl, DEFAULT_RESULT_TTL)

        @job('default', result_ttl=10)
        def hello():
            return 'Why hello'
        result = hello.delay()
        self.assertEqual(result.result_ttl, 10)

    def test_decorator_accepts_ttl_as_argument(self):
        """Ensure that passing in ttl to the decorator sets the ttl on the job
        """
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.ttl, None)

        @job('default', ttl=30)
        def hello():
            return 'Hello'
        result = hello.delay()
        self.assertEqual(result.ttl, 30)

    def test_decorator_accepts_result_depends_on_as_argument(self):
        """Ensure that passing in depends_on to the decorator sets the
        correct dependency on the job
        """

        @job(queue='queue_name')
        def foo():
            return 'Firstly'

        @job(queue='queue_name')
        def bar():
            return 'Secondly'

        foo_job = foo.delay()
        bar_job = bar.delay(depends_on=foo_job)

        self.assertIsNone(foo_job._dependency_id)

        self.assertEqual(bar_job.dependency, foo_job)

        self.assertEqual(bar_job._dependency_id, foo_job.id)

    @mock.patch('rq.queue.resolve_connection')
    def test_decorator_connection_laziness(self, resolve_connection):
        """Ensure that job decorator resolve connection in `lazy` way """

        resolve_connection.return_value = StrictRedis()

        @job(queue='queue_name')
        def foo():
            return 'do something'

        self.assertEqual(resolve_connection.call_count, 0)

        foo()

        self.assertEqual(resolve_connection.call_count, 0)

        foo.delay()

        self.assertEqual(resolve_connection.call_count, 1)

    def test_decorator_custom_queue_class(self):
        """Ensure that a custom queue class can be passed to the job decorator"""
        class CustomQueue(Queue):
            pass
        CustomQueue.enqueue_call = mock.MagicMock(
            spec=lambda *args, **kwargs: None,
            name='enqueue_call'
        )

        custom_decorator = job(queue='default', queue_class=CustomQueue)
        self.assertIs(custom_decorator.queue_class, CustomQueue)

        @custom_decorator
        def custom_queue_class_job(x, y):
            return x + y

        custom_queue_class_job.delay(1, 2)
        self.assertEqual(CustomQueue.enqueue_call.call_count, 1)

    def test_decorate_custom_queue(self):
        """Ensure that a custom queue instance can be passed to the job decorator"""
        class CustomQueue(Queue):
            pass
        CustomQueue.enqueue_call = mock.MagicMock(
            spec=lambda *args, **kwargs: None,
            name='enqueue_call'
        )
        queue = CustomQueue()

        @job(queue=queue)
        def custom_queue_job(x, y):
            return x + y

        custom_queue_job.delay(1, 2)
        self.assertEqual(queue.enqueue_call.call_count, 1)
