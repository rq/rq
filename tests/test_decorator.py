from unittest import mock

from rq.decorators import job
from rq.job import Job, Retry
from rq.queue import Queue
from rq.worker import DEFAULT_RESULT_TTL
from tests import RQTestCase


class TestDecorator(RQTestCase):
    def setUp(self):
        super().setUp()

        @job(queue='default', connection=self.connection)
        def decorated_job(x, y):
            return x + y

        self.decorated_job = decorated_job

    def test_decorator_preserves_functionality(self):
        """Ensure that a decorated function's functionality is still preserved."""
        self.assertEqual(self.decorated_job(1, 2), 3)

    def test_decorator_adds_delay_attr(self):
        """Ensure that decorator adds a delay attribute to function that returns
        a Job instance when called.
        """
        self.assertTrue(hasattr(self.decorated_job, 'delay'))
        job = self.decorated_job.enqueue(1, 2)
        self.assertTrue(isinstance(job, Job))

    def test_decorator_accepts_queue_name_as_argument(self):
        """Ensure that passing in queue name to the decorator puts the job in
        the right queue.
        """

        @job(queue='queue_name', connection=self.connection)
        def hello():
            return 'Hi'

        result = hello.enqueue()
        self.assertEqual(result.origin, 'queue_name')

    def test_decorator_accepts_result_ttl_as_argument(self):
        """Ensure that passing in result_ttl to the decorator sets the
        result_ttl on the job
        """
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.result_ttl, DEFAULT_RESULT_TTL)

        @job('default', result_ttl=10, connection=self.connection)
        def hello():
            return 'Why hello'

        result = hello.enqueue()
        self.assertEqual(result.result_ttl, 10)

    def test_decorator_accepts_ttl_as_argument(self):
        """Ensure that passing in ttl to the decorator sets the ttl on the job"""
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.ttl, None)

        @job('default', ttl=30, connection=self.connection)
        def hello():
            return 'Hello'

        result = hello.enqueue()
        self.assertEqual(result.ttl, 30)

    def test_decorator_accepts_meta_as_argument(self):
        """Ensure that passing in meta to the decorator sets the meta on the job"""
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.meta, {})

        test_meta = {
            'metaKey1': 1,
            'metaKey2': 2,
        }

        @job('default', connection=self.connection, meta=test_meta)
        def hello():
            return 'Hello'

        result = hello.enqueue()
        self.assertEqual(result.meta, test_meta)

    def test_decorator_accepts_result_depends_on_as_argument(self):
        """Ensure that passing in depends_on to the decorator sets the
        correct dependency on the job
        """
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.dependency, None)
        self.assertEqual(result._dependency_id, None)

        @job(queue='queue_name', connection=self.connection)
        def foo():
            return 'Firstly'

        foo_job = foo.enqueue()

        @job(queue='queue_name', connection=self.connection, depends_on=foo_job)
        def bar():
            return 'Secondly'

        bar_job = bar.enqueue()

        self.assertEqual(foo_job._dependency_ids, [])
        self.assertIsNone(foo_job._dependency_id)

        self.assertEqual(foo_job.dependency, None)
        self.assertEqual(bar_job.dependency, foo_job)
        self.assertEqual(bar_job.dependency.id, foo_job.id)

    def test_decorator_delay_accepts_depends_on_as_argument(self):
        """Ensure that passing in depends_on to the delay method of
        a decorated function overrides the depends_on set in the
        constructor.
        """
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.dependency, None)
        self.assertEqual(result._dependency_id, None)

        @job(queue='queue_name', connection=self.connection)
        def foo():
            return 'Firstly'

        @job(queue='queue_name', connection=self.connection)
        def bar():
            return 'Firstly'

        foo_job = foo.enqueue()
        bar_job = bar.enqueue()

        @job(queue='queue_name', connection=self.connection, depends_on=foo_job)
        def baz():
            return 'Secondly'

        baz_job = bar.enqueue(depends_on=bar_job)

        self.assertIsNone(foo_job._dependency_id)
        self.assertIsNone(bar_job._dependency_id)

        self.assertEqual(foo_job._dependency_ids, [])
        self.assertEqual(bar_job._dependency_ids, [])
        self.assertEqual(baz_job._dependency_id, bar_job.id)
        self.assertEqual(baz_job.dependency, bar_job)
        self.assertEqual(baz_job.dependency.id, bar_job.id)

    def test_decorator_accepts_on_failure_function_as_argument(self):
        """Ensure that passing in on_failure function to the decorator sets the
        correct on_failure function on the job.
        """

        # Only functions and builtins are supported as callback
        @job('default', connection=self.connection, on_failure=Job.fetch)
        def foo():
            return 'Foo'

        with self.assertRaises(ValueError):
            result = foo.enqueue()

        @job('default', connection=self.connection, on_failure=print)
        def hello():
            return 'Hello'

        result = hello.enqueue()
        result_job = Job.fetch(id=result.id, connection=self.connection)
        self.assertEqual(result_job.failure_callback, print)

    def test_decorator_accepts_on_success_function_as_argument(self):
        """Ensure that passing in on_failure function to the decorator sets the
        correct on_success function on the job.
        """

        # Only functions and builtins are supported as callback
        @job('default', connection=self.connection, on_failure=Job.fetch)
        def foo():
            return 'Foo'

        with self.assertRaises(ValueError):
            result = foo.enqueue()

        @job('default', connection=self.connection, on_success=print)
        def hello():
            return 'Hello'

        result = hello.enqueue()
        result_job = Job.fetch(id=result.id, connection=self.connection)
        self.assertEqual(result_job.success_callback, print)

    def test_decorator_custom_queue_class(self):
        """Ensure that a custom queue class can be passed to the job decorator"""

        class CustomQueue(Queue):
            pass

        CustomQueue.enqueue_call = mock.MagicMock(spec=lambda *args, **kwargs: None, name='enqueue_call')

        custom_decorator = job(queue='default', connection=self.connection, queue_class=CustomQueue)
        self.assertIs(custom_decorator.queue_class, CustomQueue)

        @custom_decorator
        def custom_queue_class_job(x, y):
            return x + y

        custom_queue_class_job.enqueue(1, 2)
        self.assertEqual(CustomQueue.enqueue_call.call_count, 1)

    def test_decorate_custom_queue(self):
        """Ensure that a custom queue instance can be passed to the job decorator"""

        class CustomQueue(Queue):
            pass

        CustomQueue.enqueue_call = mock.MagicMock(spec=lambda *args, **kwargs: None, name='enqueue_call')
        queue = CustomQueue(connection=self.connection)

        @job(queue=queue, connection=self.connection)
        def custom_queue_job(x, y):
            return x + y

        custom_queue_job.enqueue(1, 2)
        self.assertEqual(queue.enqueue_call.call_count, 1)

    def test_decorator_custom_failure_ttl(self):
        """Ensure that passing in failure_ttl to the decorator sets the
        failure_ttl on the job
        """
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.failure_ttl, None)

        @job('default', connection=self.connection, failure_ttl=10)
        def hello():
            return 'Why hello'

        result = hello.enqueue()
        self.assertEqual(result.failure_ttl, 10)

    def test_decorator_custom_retry(self):
        """Ensure that passing in retry to the decorator sets the
        retry on the job
        """
        # Ensure default
        result = self.decorated_job.enqueue(1, 2)
        self.assertEqual(result.retries_left, None)
        self.assertEqual(result.retry_intervals, None)

        @job('default', connection=self.connection, retry=Retry(3, [2]))
        def hello():
            return 'Why hello'

        result = hello.enqueue()
        self.assertEqual(result.retries_left, 3)
        self.assertEqual(result.retry_intervals, [2])
