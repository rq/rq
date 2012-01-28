from tests import RQTestCase
from pickle import dumps, loads
from rq.job import Job
#from rq import Queue, Worker
from rq.exceptions import UnpickleError


def arbitrary_function(x, y, z=1):
    return x * y / z


class TestJob(RQTestCase):
    def test_create_job(self):
        """Creation of jobs."""
        job = Job(arbitrary_function, 3, 4, z=2)
        self.assertEquals(job.func, arbitrary_function)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, {'z': 2})
        self.assertIsNone(job.origin)
        self.assertIsNone(job.timestamp)
        self.assertIsNone(job.rv_key)

    def test_pickle_job(self):
        """Pickling of jobs."""
        job = Job(arbitrary_function, 3, 4, z=2)
        job2 = loads(dumps(job))
        self.assertEquals(job.func, job2.func)
        self.assertEquals(job.args, job2.args)
        self.assertEquals(job.kwargs, job2.kwargs)

    def test_unpickle_errors(self):
        """Handling of unpickl'ing errors."""
        with self.assertRaises(UnpickleError):
            Job.unpickle('this is no pickle data')

        with self.assertRaises(UnpickleError):
            Job.unpickle(13)

        pickle_data = dumps(Job(arbitrary_function, 2, 3))
        corrupt_data = pickle_data.replace('arbitrary', 'b0rken')
        with self.assertRaises(UnpickleError):
            Job.unpickle(corrupt_data)

