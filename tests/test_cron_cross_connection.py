from rq import Queue, SimpleWorker, cron, cron_job_registry
from rq.cron import CronJob, CronScheduler, get_cron_job_ids
from tests import RQTestCase, find_empty_redis_database
from tests.fixtures import say_hello


class TestCronConnection(RQTestCase):
    def setUp(self):
        super().setUp()
        self.connection.set('test:source-in-use', '1')
        self.target = find_empty_redis_database()
        cron._job_data_registry.clear()

    def tearDown(self):
        self.target.flushdb()
        self.target.close()
        cron._job_data_registry.clear()
        super().tearDown()

    def test_direct_registration_routes_jobs_and_history(self):
        scheduler = CronScheduler(connection=self.connection)
        custom = scheduler.register(say_hello, 'maintenance', args=('target',), interval=60, connection=self.target)
        default = scheduler.register(say_hello, 'maintenance', args=('source',), interval=60)
        scheduler.enqueue_jobs()
        source_queue = Queue('maintenance', connection=self.connection)
        target_queue = Queue('maintenance', connection=self.target)
        assert source_queue.count == target_queue.count == 1
        target_job = target_queue.get_jobs()[0]
        source_job = source_queue.get_jobs()[0]
        assert get_cron_job_ids(custom.name, self.target) == [target_job.id]
        assert get_cron_job_ids(default.name, self.connection) == [source_job.id]
        assert not self.connection.exists(target_job.key)
        assert not self.target.exists(source_job.key)
        scheduler.save()
        restored = CronScheduler.fetch(scheduler.name, connection=self.connection)
        assert len(restored.get_jobs()) == 2
        assert 'connection' not in custom.to_dict()
        assert self.target.ttl(custom.job_history_key) > 0
        assert custom.name in cron_job_registry.get_names(self.target)
        assert not self.target.exists(scheduler.key)
        SimpleWorker([target_queue], connection=self.target).work(burst=True)
        target_job.refresh()
        assert target_job.is_finished
        assert source_queue.count == 1

    def test_module_registration_forwards_connection(self):
        data = cron.register(say_hello, 'maintenance', interval=60, connection=self.target)
        assert data['connection'] is self.target
        scheduler = cron.create_cron(connection=self.connection)
        scheduler.enqueue_jobs()
        assert Queue('maintenance', connection=self.target).count == 1
        assert Queue('maintenance', connection=self.connection).count == 0

    def test_direct_cron_job_connection_override(self):
        job = CronJob('maintenance', func=say_hello, interval=60, connection=self.target)
        enqueued = job.enqueue(self.connection)
        assert Queue('maintenance', connection=self.target).job_ids == [enqueued.id]
        assert Queue('maintenance', connection=self.connection).count == 0
        assert job.name in cron_job_registry.get_names(self.target)
        assert not cron_job_registry.get_names(self.connection)

    def test_explicit_none_uses_scheduler_connection(self):
        scheduler = CronScheduler(connection=self.connection)
        scheduler.register(say_hello, 'maintenance', interval=60, connection=None)
        scheduler.enqueue_jobs()
        assert Queue('maintenance', connection=self.connection).count == 1
        assert Queue('maintenance', connection=self.target).count == 0
