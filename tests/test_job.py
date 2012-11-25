import times
from datetime import datetime
from tests import RQTestCase
from tests.fixtures import Calculator, some_calculation, say_hello, access_self
from tests.helpers import strip_milliseconds
from cPickle import loads
from rq.job import Job, get_current_job
from rq.exceptions import NoSuchJobError, UnpickleError
from rq.queue import Queue


class TestJob(RQTestCase):
    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job()

        # Jobs have a random UUID and a creation date
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)

        # ...and nothing else
        self.assertIsNone(job.func)
        self.assertIsNone(job.instance)
        self.assertIsNone(job.args)
        self.assertIsNone(job.kwargs)
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.ended_at)
        self.assertIsNone(job.result)
        self.assertIsNone(job.exc_info)

    def test_create_typical_job(self):
        """Creation of jobs for function calls."""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Jobs have a random UUID
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)
        self.assertIsNotNone(job.description)
        self.assertIsNone(job.instance)

        # Job data is set...
        self.assertEquals(job.func, some_calculation)
        self.assertEquals(job.args, (3, 4))
        self.assertEquals(job.kwargs, {'z': 2})

        # ...but metadata is not
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.result)

    def test_create_instance_method_job(self):
        """Creation of jobs for instance methods."""
        c = Calculator(2)
        job = Job.create(func=c.calculate, args=(3, 4))

        # Job data is set
        self.assertEquals(job.func, c.calculate)
        self.assertEquals(job.instance, c)
        self.assertEquals(job.args, (3, 4))

    def test_create_job_from_string_function(self):
        """Creation of jobs using string specifier."""
        job = Job.create(func='tests.fixtures.say_hello', args=('World',))

        # Job data is set
        self.assertEquals(job.func, say_hello)
        self.assertIsNone(job.instance)
        self.assertEquals(job.args, ('World',))

    def test_save(self):  # noqa
        """Storing jobs."""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Saving creates a Redis hash
        self.assertEquals(self.testconn.exists(job.key), False)
        job.save()
        self.assertEquals(self.testconn.type(job.key), 'hash')

        # Saving writes pickled job data
        unpickled_data = loads(self.testconn.hget(job.key, 'data'))
        self.assertEquals(unpickled_data[0], 'tests.fixtures.some_calculation')

    def test_fetch(self):
        """Fetching jobs."""
        # Prepare test
        self.testconn.hset('rq:job:some_id', 'data',
                "(S'tests.fixtures.some_calculation'\nN(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n.")  # noqa
        self.testconn.hset('rq:job:some_id', 'created_at',
                "2012-02-07 22:13:24+0000")

        # Fetch returns a job
        job = Job.fetch('some_id')
        self.assertEquals(job.id, 'some_id')
        self.assertEquals(job.func_name, 'tests.fixtures.some_calculation')
        self.assertIsNone(job.instance)
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
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
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
        """Store, then fetch."""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
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
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
        job.save()

        # Just replace the data hkey with some random noise
        self.testconn.hset(job.key, 'data', 'this is no pickle string')
        with self.assertRaises(UnpickleError):
            job.refresh()

    def test_job_is_unimportable(self):
        """Jobs that cannot be imported throw exception on access."""
        job = Job.create(func=say_hello, args=('Lionel',))
        job.save()

        # Now slightly modify the job to make it unimportable (this is
        # equivalent to a worker not having the most up-to-date source code
        # and unable to import the function)
        data = self.testconn.hget(job.key, 'data')
        unimportable_data = data.replace('say_hello', 'shut_up')
        self.testconn.hset(job.key, 'data', unimportable_data)

        job.refresh()
        with self.assertRaises(AttributeError):
            job.func  # accessing the func property should fail

    def test_additional_job_attrs_is_persisted(self):
        """
        Verify that additional attributes stored on jobs are:
        - Saved in Redis when job.save() is called
        - Attached back to job instance when job.refresh() is called
        """
        job = Job.create(func=say_hello, args=('Lionel',))
        job.foo = 'bar'
        job.save()
        self.assertEqual(self.testconn.hget(job.key, 'foo'), 'bar')

        job2 = Job.fetch(job.id)
        job2.refresh()
        self.assertEqual(job2.foo, 'bar')

    def test_result_ttl_is_persisted(self):
        """Ensure that job's result_ttl is set properly"""
        job = Job.create(func=say_hello, args=('Lionel',), result_ttl=10)
        job.save()
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, 10)

        job = Job.create(func=say_hello, args=('Lionel',))
        job.save()
        job_from_queue = Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, None)

    def test_job_access_within_job_function(self):
        """The current job is accessible within the job function."""
        # Executing the job function from outside of RQ throws an exception
        self.assertIsNone(get_current_job())

        # Executing the job function from within the job works (and in
        # this case leads to the job ID being returned)
        job = Job.create(func=access_self)
        job.save()
        id = job.perform()
        self.assertEqual(job.id, id)
        self.assertEqual(job.func, access_self)

        # Ensure that get_current_job also works from within synchronous jobs
        queue = Queue(async=False)
        job = queue.enqueue(access_self)
        id = job.perform()
        self.assertEqual(job.id, id)
        self.assertEqual(job.func, access_self)