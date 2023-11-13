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
        batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        assert isinstance(batch, Batch)
        assert len(batch.get_jobs()) == 2
        q.empty

    def test_batch_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        self.assertCountEqual(batch.get_jobs(), jobs)
        q.empty()

    def test_fetch_batch(self):
        q = Queue(connection=self.testconn)
        enqueued_batch = Batch.create(connection=self.testconn)
        enqueued_batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        fetched_batch = Batch.fetch(enqueued_batch.id, self.testconn)
        self.assertCountEqual(enqueued_batch.get_jobs(), fetched_batch.get_jobs())
        assert len(fetched_batch.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job2 = batch.enqueue_many(q, [self.job_1_data, self.job_2_data])[0]
        assert job2 in batch.get_jobs()
        self.assertEqual(job2.batch_id, batch.id)
        q.empty()

    def test_jobs_added_to_batch_key(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job_ids = [job.id for job in batch.get_jobs()]
        jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_batch_id_added_to_jobs(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        jobs = batch.enqueue_many(q, [self.job_1_data])
        assert jobs[0].batch_id == batch.id
        fetched_job = Job.fetch(jobs[0].id, connection=self.testconn)
        assert fetched_job.batch_id == batch.id

    def test_deleted_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job = batch.get_jobs()[0]
        job.delete()
        batch.cleanup()
        redis_jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        assert job.id not in redis_jobs
        assert job not in batch.get_jobs()

    def test_batch_added_to_registry(self):
        q = Queue(connection=self.testconn)
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many(q, [self.job_1_data])
        redis_batches = {as_text(batch) for batch in self.testconn.smembers("rq:batches")}
        assert batch.id in redis_batches
        q.empty()

    def test_expired_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many(q, [short_lived_job, self.job_1_data])
        w.work(burst=True, max_jobs=1)
        sleep(3)
        batch.cleanup()
        assert len(batch.get_jobs()) == 1
        q.empty()

    def test_batch_with_str_queue(self):
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many("new_queue", [self.job_1_data])
        q = Queue("new_queue", connection=self.testconn)
        assert q.count == 1
        q.empty()

    def test_empty_batch_removed_from_batch_list(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = Batch.create(connection=self.testconn)
        batch.enqueue_many(q, [short_lived_job])
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
        batch.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(3)
        self.assertRaises(NoSuchBatchError, Batch.fetch, batch.id, batch.connection)
        q.empty()

    def test_get_batch_key(self):
        batch = Batch(id="foo", connection=self.testconn)
        self.assertEqual(Batch.get_key(batch.id), "rq:batch:foo")

    def test_all_returns_all_batches(self):
        q = Queue(connection=self.testconn)
        batch1 = Batch.create(id="batch1", connection=self.testconn)
        Batch.create(id="batch2", connection=self.testconn)
        batch1.enqueue_many(q, [self.job_1_data, self.job_2_data])
        all_batches = Batch.all(self.testconn)
        assert len(all_batches) == 1
        assert "batch1" in [batch.id for batch in all_batches]
        assert "batch2" not in [batch.id for batch in all_batches]
