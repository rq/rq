from time import sleep

from rq import Queue, SimpleWorker
from rq.batch import Batch
from rq.exceptions import NoSuchBatchError
from rq.job import Job
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import say_hello


class TestBatch(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')

    def test_create_batch(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([self.job_1_data, self.job_2_data], batch=batch)
        assert isinstance(batch, Batch)
        assert len(batch.get_jobs()) == 2
        q.empty

    def test_batch_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = q.enqueue_many([self.job_1_data, self.job_2_data], batch=batch)
        self.assertCountEqual(batch.get_jobs(), jobs)
        q.empty()

    def test_fetch_batch(self):
        q = Queue(connection=self.testconn)
        enqueued_batch = Batch.create(connection=self.testconn)
        q.enqueue_many([self.job_1_data, self.job_2_data], batch=enqueued_batch)
        fetched_batch = Batch.fetch(enqueued_batch.id, self.testconn)
        self.assertCountEqual(enqueued_batch.get_jobs(), fetched_batch.get_jobs())
        assert len(fetched_batch.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([self.job_1_data], batch=batch)[0]
        job2 = q.enqueue_many([self.job_2_data], batch=batch)[0]
        assert job2 in batch.get_jobs()
        self.assertEqual(job2.batch_id, batch.id)
        q.empty()

    def test_jobs_added_to_batch_key(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = q.enqueue_many([self.job_1_data, self.job_2_data], batch=batch)
        job_ids = [job.id for job in batch.get_jobs()]
        jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_batch_id_added_to_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = q.enqueue_many([self.job_1_data], batch=batch)
        assert jobs[0].batch_id == batch.id
        fetched_job = Job.fetch(jobs[0].id, connection=self.testconn)
        assert fetched_job.batch_id == batch.id

    def test_deleted_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([self.job_1_data, self.job_2_data], batch=batch)
        job = batch.get_jobs()[0]
        job.delete()
        batch.cleanup()
        redis_jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        assert job.id not in redis_jobs
        assert job not in batch.get_jobs()

    def test_batch_added_to_registry(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([self.job_1_data], batch=batch)
        redis_batches = {as_text(batch) for batch in self.testconn.smembers("rq:batches")}
        assert batch.id in redis_batches
        q.empty()

    def test_expired_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([short_lived_job, self.job_1_data], batch=batch)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        batch.cleanup()
        assert len(batch.get_jobs()) == 1
        q.empty()

    def test_empty_batch_removed_from_batch_list(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([short_lived_job], batch=batch)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        w.run_maintenance_tasks()
        redis_batches = {as_text(batch) for batch in self.testconn.smembers("rq:batches")}
        assert batch.id not in redis_batches

    def test_fetch_expired_batch_raises_error(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = Batch.create(connection=self.testconn)
        q.enqueue_many([short_lived_job], batch=batch)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        self.assertRaises(NoSuchBatchError, Batch.fetch, batch.id, batch.connection)
        q.empty()

    def test_get_batch_key(self):
        batch = Batch(id="foo", connection=self.testconn)
        self.assertEqual(Batch.get_key(batch.id), "rq:batch:foo")

    def test_all_returns_all_batches(self):
        q = Queue(connection=self.testconn)
        Batch.create(id="batch1", connection=self.testconn)
        q.enqueue_many([self.job_1_data, self.job_2_data], batch="batch1")
        Batch(id="batch2", connection=self.testconn)
        q.enqueue_many([self.job_1_data, self.job_2_data], batch="batch2")
        all_batches = Batch.all(self.testconn)
        assert len(all_batches) == 2
        assert "batch1" in [batch.id for batch in all_batches]
        assert "batch2" in [batch.id for batch in all_batches]
