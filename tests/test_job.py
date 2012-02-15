import times
from datetime import datetime
from tests import RQTestCase
from tests.helpers import strip_milliseconds
from cPickle import loads
from rq import Job
from rq.exceptions import NoSuchJobError, UnpickleError


def arbitrary_function(x, y, z=1):
    return x * y / z


class TestJob(RQTestCase):
    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job()

        # Jobs have a random UUID and a creation date
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)

        # ...and nothing else
        self.assertIsNone(job.func, None)
        self.assertIsNone(job.args, None)
        self.assertIsNone(job.kwargs, None)
        self.assertIsNone(job.origin, None)
        self.assertIsNone(job.enqueued_at, None)
        self.assertIsNone(job.ended_at, None)
        self.assertIsNone(job.return_value, None)
        self.assertIsNone(job.exc_info, None)

    def test_create_typical_job(self):
        """Creation of jobs for function calls."""
        job = Job.create(arbitrary_function, 3, 4, z=2)

        # Jobs have a random UUID
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)
        self.assertIsNotNone(job.description)

        # Job data is set...
        self.assertEquals(job.func, arbitrary_function)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, {'z': 2})

        # ...but metadata is not
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.return_value)


    def test_save(self):  # noqa
        """Storing jobs."""
        job = Job.create(arbitrary_function, 3, 4, z=2)

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
        self.testconn.hset('rq:job:some_id', 'data',
                "(ctest_job\narbitrary_function\np0\n(I3\nI4\ntp1\n(dp2\nS'z'\np3\nI2\nstp4\n.")  # noqa
        self.testconn.hset('rq:job:some_id', 'created_at',
                "2012-02-07 22:13:24+0000")

        # Fetch returns a job
        job = Job.fetch('some_id')
        self.assertEquals(job.id, 'some_id')
        self.assertEquals(job.func, arbitrary_function)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, dict(z=2))
        self.assertEquals(job.created_at, datetime(2012, 2, 7, 22, 13, 24))


    def test_persistence_of_empty_jobs(self):  # noqa
        """Storing empty jobs."""
        job = Job()
        job.save()

        expected_date = strip_milliseconds(job.created_at)
        stored_date = self.testconn.hget(job.key, 'created_at')
        self.assertEquals(
                times.to_universal(stored_date),
                expected_date)

        # ... and no other keys are stored
        self.assertItemsEqual(
                self.testconn.hkeys(job.key),
                ['created_at'])

    def test_persistence_of_typical_jobs(self):
        """Storing typical jobs."""
        job = Job.create(arbitrary_function, 3, 4, z=2)
        job.save()

        expected_date = strip_milliseconds(job.created_at)
        stored_date = self.testconn.hget(job.key, 'created_at')
        self.assertEquals(
                times.to_universal(stored_date),
                expected_date)

        # ... and no other keys are stored
        self.assertItemsEqual(
                self.testconn.hkeys(job.key),
                ['created_at', 'data', 'description'])

    def test_store_then_fetch(self):
        job = Job.create(arbitrary_function, 3, 4, z=2)
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

    def test_fetching_unreadable_data(self):
        """Fetching fails on unreadable data."""
        # Set up
        job = Job.create(arbitrary_function, 3, 4, z=2)
        job.save()

        # Just replace the data hkey with some random noise
        self.testconn.hset(job.key, 'data', 'this is no pickle string')
        with self.assertRaises(UnpickleError):
            job.refresh()

        # Set up (part B)
        job = Job.create(arbitrary_function, 3, 4, z=2)
        job.save()

        # Now slightly modify the job to make it unpickl'able (this is
        # equivalent to a worker not having the most up-to-date source code and
        # unable to import the function)
        data = self.testconn.hget(job.key, 'data')
        unimportable_data = data.replace('arbitrary_function', 'broken')
        self.testconn.hset(job.key, 'data', unimportable_data)
        with self.assertRaises(UnpickleError):
            job.refresh()

