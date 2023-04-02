from time import sleep

from tests import RQTestCase
from tests.fixtures import _send_shutdown_command

from rq.queue import Queue
from rq.worker_pool import Pool


class TestWorkerPool(RQTestCase):
    def test_queues(self):
        """Test queue parsing"""
        pool = Pool(['default', 'foo'], connection=self.connection)
        self.assertEqual(
            set(pool.queues), {Queue('default', connection=self.connection), Queue('foo', connection=self.connection)}
        )

    def test_spawn_workers(self):
        """Test spawning workers"""
        pool = Pool(['default', 'foo'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=True, _sleep=1)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

    def test_check_workers(self):
        """Test check_workers()"""
        pool = Pool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.check_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        _send_shutdown_command(worker_data.name, self.connection.connection_pool.connection_kwargs.copy(), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.2)
        pool.check_workers(respawn=False)
        self.assertEqual(len(pool.worker_dict.keys()), 1)

        # If we call `check_workers` with `respawn=True`, the worker should be respawned
        pool.check_workers(respawn=True)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        pool.stop_workers()

    def test_reap_workers(self):
        """Dead workers are removed from worker_dict"""
        pool = Pool(['default'], connection=self.connection, num_workers=2)
        pool.start_workers(burst=False)

        # There should be two workers
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        worker_data = list(pool.worker_dict.values())[0]
        _send_shutdown_command(worker_data.name, self.connection.connection_pool.connection_kwargs.copy(), delay=0)
        # 1 worker should be dead since we sent a shutdown command
        sleep(0.2)
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 1)
        pool.stop_workers()
