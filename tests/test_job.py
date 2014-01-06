from datetime import datetime
from tests import RQTestCase
from tests.fixtures import Number, some_calculation, say_hello, access_self
from tests.helpers import strip_microseconds
try:
    from cPickle import loads, dumps
except ImportError:
    from pickle import loads, dumps
from rq.compat import as_text
from rq.job import Job, get_current_job
from rq.exceptions import NoSuchJobError, UnpickleError
from rq.queue import Queue
from rq.utils import utcformat


class TestJob(RQTestCase):
    def test_create_empty_job(self):
        """Creation of new empty jobs."""
        job = Job()

        # Jobs have a random UUID and a creation date
        self.assertIsNotNone(job.id)
        self.assertIsNotNone(job.created_at)

        # ...and nothing else
        self.assertIsNone(job.origin)
        self.assertIsNone(job.enqueued_at)
        self.assertIsNone(job.ended_at)
        self.assertIsNone(job.result)
        self.assertIsNone(job.exc_info)

        with self.assertRaises(ValueError):
            job.func
        with self.assertRaises(ValueError):
            job.instance
        with self.assertRaises(ValueError):
            job.args
        with self.assertRaises(ValueError):
            job.kwargs

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
        n = Number(2)
        job = Job.create(func=n.div, args=(4,))

        # Job data is set
        self.assertEquals(job.func, n.div)
        self.assertEquals(job.instance, n)
        self.assertEquals(job.args, (4,))

    def test_create_job_from_string_function(self):
        """Creation of jobs using string specifier."""
        job = Job.create(func='tests.fixtures.say_hello', args=('World',))

        # Job data is set
        self.assertEquals(job.func, say_hello)
        self.assertIsNone(job.instance)
        self.assertEquals(job.args, ('World',))

    def test_job_properties_set_data_property(self):
        """Data property gets derived from the job tuple."""
        job = Job()
        job.func_name = 'foo'
        fname, instance, args, kwargs = loads(job.data)

        self.assertEquals(fname, job.func_name)
        self.assertEquals(instance, None)
        self.assertEquals(args, ())
        self.assertEquals(kwargs, {})

    def test_data_property_sets_job_properties(self):
        """Job tuple gets derived lazily from data property."""
        job = Job()
        job.data = dumps(('foo', None, (1, 2, 3), {'bar': 'qux'}))

        self.assertEquals(job.func_name, 'foo')
        self.assertEquals(job.instance, None)
        self.assertEquals(job.args, (1, 2, 3))
        self.assertEquals(job.kwargs, {'bar': 'qux'})

    def test_save(self):  # noqa
        """Storing jobs."""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))

        # Saving creates a Redis hash
        self.assertEquals(self.testconn.exists(job.key), False)
        job.save()
        self.assertEquals(self.testconn.type(job.key), b'hash')

        # Saving writes pickled job data
        unpickled_data = loads(self.testconn.hget(job.key, 'data'))
        self.assertEquals(unpickled_data[0], 'tests.fixtures.some_calculation')

    def test_fetch(self):
        """Fetching jobs."""
        # Prepare test
        self.testconn.hset('rq:job:some_id', 'data',
                           "(S'tests.fixtures.some_calculation'\nN(I3\nI4\nt(dp1\nS'z'\nI2\nstp2\n.")
        self.testconn.hset('rq:job:some_id', 'created_at',
                           '2012-02-07T22:13:24Z')

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
        with self.assertRaises(ValueError):
            job.save()

    def test_persistence_of_typical_jobs(self):
        """Storing typical jobs."""
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
        job.save()

        expected_date = strip_microseconds(job.created_at)
        stored_date = self.testconn.hget(job.key, 'created_at').decode('utf-8')
        self.assertEquals(
            stored_date,
            utcformat(expected_date))

        # ... and no other keys are stored
        self.assertEqual(
            sorted(self.testconn.hkeys(job.key)),
            [b'created_at', b'data', b'description'])

    def test_persistence_of_parent_job(self):
        """Storing jobs with parent job, either instance or key."""
        parent_job = Job.create(func=some_calculation)
        parent_job.save()
        job = Job.create(func=some_calculation, depends_on=parent_job)
        job.save()
        stored_job = Job.fetch(job.id)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

        job = Job.create(func=some_calculation, depends_on=parent_job.id)
        job.save()
        stored_job = Job.fetch(job.id)
        self.assertEqual(stored_job._dependency_id, parent_job.id)
        self.assertEqual(stored_job.dependency, parent_job)

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
        """Fetching succeeds on unreadable data, but lazy props fail."""
        # Set up
        job = Job.create(func=some_calculation, args=(3, 4), kwargs=dict(z=2))
        job.save()

        # Just replace the data hkey with some random noise
        self.testconn.hset(job.key, 'data', 'this is no pickle string')
        job.refresh()

        for attr in ('func_name', 'instance', 'args', 'kwargs'):
            with self.assertRaises(UnpickleError):
                getattr(job, attr)

    def test_job_is_unimportable(self):
        """Jobs that cannot be imported throw exception on access."""
        job = Job.create(func=say_hello, args=('Lionel',))
        job.save()

        # Now slightly modify the job to make it unimportable (this is
        # equivalent to a worker not having the most up-to-date source code
        # and unable to import the function)
        data = self.testconn.hget(job.key, 'data')
        unimportable_data = data.replace(b'say_hello', b'shut_up')
        self.testconn.hset(job.key, 'data', unimportable_data)

        job.refresh()
        with self.assertRaises(AttributeError):
            job.func  # accessing the func property should fail

    def test_custom_meta_is_persisted(self):
        """Additional meta data on jobs are stored persisted correctly."""
        job = Job.create(func=say_hello, args=('Lionel',))
        job.meta['foo'] = 'bar'
        job.save()

        raw_data = self.testconn.hget(job.key, 'meta')
        self.assertEqual(loads(raw_data)['foo'], 'bar')

        job2 = Job.fetch(job.id)
        self.assertEqual(job2.meta['foo'], 'bar')

    def test_result_ttl_is_persisted(self):
        """Ensure that job's result_ttl is set properly"""
        job = Job.create(func=say_hello, args=('Lionel',), result_ttl=10)
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, 10)

        job = Job.create(func=say_hello, args=('Lionel',))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.result_ttl, None)

    def test_description_is_persisted(self):
        """Ensure that job's custom description is set properly"""
        job = Job.create(func=say_hello, args=('Lionel',), description=u'Say hello!')
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.description, u'Say hello!')

        # Ensure job description is constructed from function call string
        job = Job.create(func=say_hello, args=('Lionel',))
        job.save()
        Job.fetch(job.id, connection=self.testconn)
        self.assertEqual(job.description, "tests.fixtures.say_hello('Lionel')")

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

    def test_get_ttl(self):
        """Getting job TTL."""
        job_ttl = 1
        default_ttl = 2
        job = Job.create(func=say_hello, result_ttl=job_ttl)
        job.save()
        self.assertEqual(job.get_ttl(default_ttl=default_ttl), job_ttl)
        self.assertEqual(job.get_ttl(), job_ttl)
        job = Job.create(func=say_hello)
        job.save()
        self.assertEqual(job.get_ttl(default_ttl=default_ttl), default_ttl)
        self.assertEqual(job.get_ttl(), None)

    def test_cleanup(self):
        """Test that jobs and results are expired properly."""
        job = Job.create(func=say_hello)
        job.save()

        # Jobs with negative TTLs don't expire
        job.cleanup(ttl=-1)
        self.assertEqual(self.testconn.ttl(job.key), -1)

        # Jobs with positive TTLs are eventually deleted
        job.cleanup(ttl=100)
        self.assertEqual(self.testconn.ttl(job.key), 100)

        # Jobs with 0 TTL are immediately deleted
        job.cleanup(ttl=0)
        self.assertRaises(NoSuchJobError, Job.fetch, job.id, self.testconn)

    def test_register_dependency(self):
        """Test that jobs updates the correct job dependents."""
        job = Job.create(func=say_hello)
        job._dependency_id = 'id'
        job.save()
        job.register_dependency()
        self.assertEqual(as_text(self.testconn.spop('rq:job:id:dependents')), job.id)

    def test_cancel(self):
        """job.cancel() deletes itself & dependents mapping from Redis."""
        job = Job.create(func=say_hello)
        job2 = Job.create(func=say_hello, depends_on=job)
        job2.register_dependency()
        job.cancel()
        self.assertFalse(self.testconn.exists(job.key))
        self.assertFalse(self.testconn.exists(job.dependents_key))
