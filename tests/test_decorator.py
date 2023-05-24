from unittest import mock

from redis import Redis

from rq.decorators import job
from rq.job import Job, Retry
from rq.queue import Queue
from rq.worker import DEFAULT_RESULT_TTL
from tests import RQTestCase
from tests.fixtures import decorated_job


class TestDecorator(RQTestCase):
    def setUp(self):
        super().setUp()

    def test_decorator_preserves_functionality(self):
        """Ensure that a decorated function's functionality is still preserved."""
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
        """Ensure that passing in ttl to the decorator sets the ttl on the job"""
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.ttl, None)

        @job('default', ttl=30)
        def hello():
            return 'Hello'

        result = hello.delay()
        self.assertEqual(result.ttl, 30)

    def test_decorator_accepts_meta_as_argument(self):
        """Ensure that passing in meta to the decorator sets the meta on the job"""
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.meta, {})

        test_meta = {
            'metaKey1': 1,
            'metaKey2': 2,
        }

        @job('default', meta=test_meta)
        def hello():
            return 'Hello'

        result = hello.delay()
        self.assertEqual(result.meta, test_meta)

    def test_decorator_accepts_result_depends_on_as_argument(self):
        """Ensure that passing in depends_on to the decorator sets the
        correct dependency on the job
        """
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.dependency, None)
        self.assertEqual(result._dependency_id, None)

        @job(queue='queue_name')
        def foo():
            return 'Firstly'

        foo_job = foo.delay()

        @job(queue='queue_name', depends_on=foo_job)
        def bar():
            return 'Secondly'

        bar_job = bar.delay()

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
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.dependency, None)
        self.assertEqual(result._dependency_id, None)

        @job(queue='queue_name')
        def foo():
            return 'Firstly'

        @job(queue='queue_name')
        def bar():
            return 'Firstly'

        foo_job = foo.delay()
        bar_job = bar.delay()

        @job(queue='queue_name', depends_on=foo_job)
        def baz():
            return 'Secondly'

        baz_job = bar.delay(depends_on=bar_job)

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
        @job('default', on_failure=Job.fetch)
        def foo():
            return 'Foo'

        with self.assertRaises(ValueError):
            result = foo.delay()

        @job('default', on_failure=print)
        def hello():
            return 'Hello'

        result = hello.delay()
        result_job = Job.fetch(id=result.id, connection=self.testconn)
        self.assertEqual(result_job.failure_callback, print)

    def test_decorator_accepts_on_success_function_as_argument(self):
        """Ensure that passing in on_failure function to the decorator sets the
        correct on_success function on the job.
        """

        # Only functions and builtins are supported as callback
        @job('default', on_failure=Job.fetch)
        def foo():
            return 'Foo'

        with self.assertRaises(ValueError):
            result = foo.delay()

        @job('default', on_success=print)
        def hello():
            return 'Hello'

        result = hello.delay()
        result_job = Job.fetch(id=result.id, connection=self.testconn)
        self.assertEqual(result_job.success_callback, print)

    @mock.patch('rq.queue.resolve_connection')
    def test_decorator_connection_laziness(self, resolve_connection):
        """Ensure that job decorator resolve connection in `lazy` way"""

        resolve_connection.return_value = Redis()

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

        CustomQueue.enqueue_call = mock.MagicMock(spec=lambda *args, **kwargs: None, name='enqueue_call')

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

        CustomQueue.enqueue_call = mock.MagicMock(spec=lambda *args, **kwargs: None, name='enqueue_call')
        queue = CustomQueue()

        @job(queue=queue)
        def custom_queue_job(x, y):
            return x + y

        custom_queue_job.delay(1, 2)
        self.assertEqual(queue.enqueue_call.call_count, 1)

    def test_decorator_custom_failure_ttl(self):
        """Ensure that passing in failure_ttl to the decorator sets the
        failure_ttl on the job
        """
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.failure_ttl, None)

        @job('default', failure_ttl=10)
        def hello():
            return 'Why hello'

        result = hello.delay()
        self.assertEqual(result.failure_ttl, 10)

    def test_decorator_custom_retry(self):
        """Ensure that passing in retry to the decorator sets the
        retry on the job
        """
        # Ensure default
        result = decorated_job.delay(1, 2)
        self.assertEqual(result.retries_left, None)
        self.assertEqual(result.retry_intervals, None)

        @job('default', retry=Retry(3, [2]))
        def hello():
            return 'Why hello'

        result = hello.delay()
        self.assertEqual(result.retries_left, 3)
        self.assertEqual(result.retry_intervals, [2])
