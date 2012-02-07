from datetime import datetime
from tests import RQTestCase
from pickle import dumps, loads
from rq.job import Job
from rq.exceptions import NoSuchJobError, UnpickleError


def arbitrary_function(x, y, z=1):
    return x * y / z


class TestJob(RQTestCase):
    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job()

        # Jobs have a random UUID
        self.assertIsNotNone(job.id)

        # Jobs have no data yet...
        self.assertEquals(job.func, None)
        self.assertEquals(job.args, None)
        self.assertEquals(job.kwargs, None)
        self.assertEquals(job.origin, None)
        self.assertEquals(job.enqueued_at, None)
        self.assertEquals(job.result, None)
        self.assertEquals(job.exc_info, None)

        # ...except for a created_at property
        self.assertIsNotNone(job.created_at)

    def test_create_normal_job(self):
        """Creation of jobs for function calls."""
        job = Job.for_call(arbitrary_function, 3, 4, z=2)

        # Jobs have a random UUID
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)

        # Job data is set...
        self.assertEquals(job.func, arbitrary_function)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, {'z': 2})

        # ...but metadata is not
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.result)


    def test_save(self):
        """Storing jobs."""
        job = Job.for_call(arbitrary_function, 3, 4, z=2)

        # Saving creates a Redis hash
        self.assertEquals(self.testconn.exists(job.key), False)
        job.save()
        self.assertEquals(self.testconn.type(job.key), 'hash')

        # Saving writes pickled job data
        unpickled_data = loads(self.testconn.hget(job.key, 'data'))
        self.assertEquals(unpickled_data[0], arbitrary_function)

    def test_fetch(self):
        """Fetching jobs."""
        # Prepare test
        self.testconn.hset('rq:job:some_id', 'data', "(ctest_job\narbitrary_function\np0\n(I3\nI4\ntp1\n(dp2\nS'z'\np3\nI2\nstp4\n.")
        self.testconn.hset('rq:job:some_id', 'created_at', "2012-02-07 22:13:24+0000")

        # Fetch returns a job
        job = Job.fetch('some_id')
        self.assertEquals(job.id, 'some_id')
        self.assertEquals(job.func, arbitrary_function)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, dict(z=2))
        self.assertEquals(job.created_at, datetime(2012, 2, 7, 22, 13, 24))


    def test_persistence_of_jobs(self):
        """Storing and fetching of jobs."""
        job = Job.for_call(arbitrary_function, 3, 4, z=2)
        job.save()

        job2 = Job.fetch(job.id)
        self.assertEquals(job.func, job2.func)
        self.assertEquals(job.args, job2.args)
        self.assertEquals(job.kwargs, job2.kwargs)

        # Mathematical equation
        self.assertEquals(job, job2)

    def test_fetching_can_fail(self):
        """Fetching fails for non-existing jobs."""
        with self.assertRaises(NoSuchJobError):
            Job.fetch('b4a44d44-da16-4620-90a6-798e8cd72ca0')


    def test_unpickle_errors(self):
        """Handling of unpickl'ing errors."""
        with self.assertRaises(UnpickleError):
            Job.unpickle('this is no pickle data')

        with self.assertRaises(UnpickleError):
            Job.unpickle(13)

        pickle_data = dumps(Job.for_call(arbitrary_function, 2, 3))
        corrupt_data = pickle_data.replace('arbitrary', 'b0rken')
        with self.assertRaises(UnpickleError):
            Job.unpickle(corrupt_data)

