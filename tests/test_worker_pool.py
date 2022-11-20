from time import sleep

from tests import RQTestCase

from rq.queue import Queue
from rq.worker_pool import Pool


class TestWorkerPool(RQTestCase):

    def test_queues(self):
        """Test queue parsing"""
        pool = Pool(['default', 'foo'], connection=self.connection)
        self.assertEqual(
            set(pool.queues),
            {Queue('default', connection=self.connection), Queue('foo', connection=self.connection)}
        )

    def test_spawn_workers(self):
        """Test spawning workers"""
        pool = Pool(['default', 'foo'], connection=self.connection, num_workers=2)
        pool.start_workers(sleep=2)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

    def test_check_workers(self):
        """Test spawning workers"""
        pool = Pool(['default', 'foo'], connection=self.connection, num_workers=2)
        pool.start_workers(sleep=2)
        self.assertEqual(len(pool.worker_dict.keys()), 2)

    def test_reap_workers(self):
        """Dead workers are removed from worker_dict"""
        pool = Pool(['default', 'foo'], connection=self.connection, num_workers=2)
        pool.start_workers(sleep=0.5)        

        # There are two workers since worker is only started after 0.5 seconds
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 2)

        # After waiting 1 second, the two workers should already be dead
        sleep(1)
        pool.reap_workers()
        self.assertEqual(len(pool.worker_dict.keys()), 0)
