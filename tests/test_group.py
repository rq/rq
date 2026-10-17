from time import sleep

import pytest
from redis.exceptions import WatchError

from rq import Queue, SimpleWorker
from rq.exceptions import NoSuchGroupError
from rq.group import Group
from rq.job import Job
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import say_hello


class TestGroup(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')

    def test_create_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        assert isinstance(group, Group)
        assert len(group.get_jobs()) == 2
        q.empty()

    def test_enqueue_many_defers_to_supplied_pipeline(self):
        for transaction in (True, False):
            with self.subTest(transaction=transaction):
                self.connection.flushdb()
                q = Queue(connection=self.connection)
                group = Group.create(connection=self.connection)
                with self.connection.pipeline(transaction=transaction) as pipe:
                    pipe.set('before-group', 'pending')
                    jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data], pipeline=pipe)
                    self.assertIsNone(self.connection.get('before-group'))
                    self.assertEqual(q.count, 0)
                    self.assertFalse(self.connection.exists(group.key))
                    self.assertFalse(self.connection.sismember(Group.REDIS_GROUP_KEY, group.name))
                    for job in jobs:
                        self.assertFalse(self.connection.exists(job.key))
                    pipe.set('after-group', 'pending')
                    pipe.execute()

                self.assertEqual(self.connection.get('before-group'), b'pending')
                self.assertEqual(self.connection.get('after-group'), b'pending')
                self.assertEqual(q.get_job_ids(), [job.id for job in jobs])
                self.assertCountEqual(group.get_jobs(), jobs)
                self.assertTrue(self.connection.sismember(Group.REDIS_GROUP_KEY, group.name))

    def test_enqueue_many_supplied_pipeline_can_be_discarded(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        with self.connection.pipeline() as pipe:
            jobs = group.enqueue_many(q, [self.job_1_data], pipeline=pipe)
            pipe.reset()

        self.assertEqual(q.count, 0)
        self.assertFalse(self.connection.exists(jobs[0].key))
        self.assertFalse(self.connection.exists(group.key))
        self.assertFalse(self.connection.sismember(Group.REDIS_GROUP_KEY, group.name))

    def test_enqueue_many_preserves_watched_transaction(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        self.connection.set('watched', 'original')
        with self.connection.pipeline() as pipe:
            pipe.watch('watched')
            pipe.multi()
            jobs = group.enqueue_many(q, [self.job_1_data], pipeline=pipe)
            self.connection.set('watched', 'changed')
            with self.assertRaises(WatchError):
                pipe.execute()

        self.assertEqual(q.count, 0)
        self.assertFalse(self.connection.exists(jobs[0].key))
        self.assertFalse(self.connection.exists(group.key))

    def test_group_cleanup_with_no_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        assert len(group.get_jobs()) == 0
        group.cleanup()
        assert len(group.get_jobs()) == 0
        q.empty()

    def test_group_repr(self):
        group = Group.create(name='foo', connection=self.connection)
        assert group.__repr__() == 'Group(id=foo)'

    def test_group_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        self.assertCountEqual(group.get_jobs(), jobs)
        q.empty()

    def test_fetch_group(self):
        q = Queue(connection=self.connection)
        enqueued_group = Group.create(connection=self.connection)
        enqueued_group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        fetched_group = Group.fetch(enqueued_group.name, self.connection)
        self.assertCountEqual(enqueued_group.get_jobs(), fetched_group.get_jobs())
        assert len(fetched_group.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job2 = group.enqueue_many(q, [self.job_1_data, self.job_2_data])[0]
        assert job2 in group.get_jobs()
        self.assertEqual(job2.group_id, group.name)
        q.empty()

    def test_jobs_added_to_group_key(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job_ids = [job.id for job in group.get_jobs()]
        jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_group_id_added_to_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        assert jobs[0].group_id == group.name
        fetched_job = Job.fetch(jobs[0].id, connection=self.connection)
        assert fetched_job.group_id == group.name

    def test_deleted_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job = group.get_jobs()[0]
        job.delete()
        group.cleanup()
        redis_jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        assert job.id not in redis_jobs
        assert job not in group.get_jobs()

    def test_group_added_to_registry(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data])
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name in redis_groups
        q.empty()

    @pytest.mark.slow
    def test_expired_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job, self.job_1_data])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        group.cleanup()
        assert len(group.get_jobs()) == 1
        assert self.job_1_data.job_id in [job.id for job in group.get_jobs()]
        q.empty()

    @pytest.mark.slow
    def test_empty_group_removed_from_group_list(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name not in redis_groups

    @pytest.mark.slow
    def test_fetch_expired_group_raises_error(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        self.assertRaises(NoSuchGroupError, Group.fetch, group.name, group.connection)
        q.empty()

    def test_get_group_key(self):
        group = Group(name='foo', connection=self.connection)
        self.assertEqual(Group.get_key(group.name), 'rq:group:foo')

    def test_all_returns_all_groups(self):
        q = Queue(connection=self.connection)
        group1 = Group.create(name='group1', connection=self.connection)
        Group.create(name='group2', connection=self.connection)
        group1.enqueue_many(q, [self.job_1_data, self.job_2_data])
        all_groups = Group.all(self.connection)
        assert len(all_groups) == 1
        assert 'group1' in [group.name for group in all_groups]
        assert 'group2' not in [group.name for group in all_groups]

    def test_all_deletes_missing_groups(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        jobs[0].delete()
        assert not self.connection.exists(Group.get_key(group.name))
        assert Group.all(connection=self.connection) == []
