from tests import RQTestCase
from tests.fixtures import decorated_job

from rq.decorators import job
from rq.job import Job


class TestDecorator(RQTestCase):

    def setUp(self):
        super(TestDecorator, self).setUp()

    def test_decorator_preserves_functionality(self):
        """
        Ensure that a decorated function's functionality is still preserved
        """
        self.assertEqual(decorated_job(1, 2), 3)

    def test_decorator_adds_delay_attr(self):
        """
        Ensure that decorator adds a delay attribute to function that returns
        a Job instance when called.
        """
        self.assertTrue(hasattr(decorated_job, 'delay'))
        result = decorated_job.delay(1, 2)
        self.assertTrue(isinstance(result, Job))
        # Ensure that job returns the right result when performed
        self.assertEqual(result.perform(), 3)

