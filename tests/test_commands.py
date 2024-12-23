import time
from multiprocessing import Process
from unittest import mock

from redis import Redis
from redis.exceptions import ResponseError

from rq import Queue, Worker
from rq.command import send_command, send_kill_horse_command, send_shutdown_command, send_stop_job_command
from rq.exceptions import InvalidJobOperation, NoSuchJobError
from rq.serializers import JSONSerializer
from rq.worker import WorkerStatus
from tests import RQTestCase
from tests.fixtures import _send_kill_horse_command, _send_shutdown_command, long_running_job, raise_exc_mock


def start_work(queue_name, worker_name, connection_kwargs):
    worker = Worker(queue_name, name=worker_name, connection=Redis(**connection_kwargs))
    worker.work()


def start_work_burst(queue_name, worker_name, connection_kwargs):
    worker = Worker(queue_name, name=worker_name, connection=Redis(**connection_kwargs), serializer=JSONSerializer)
    worker.work(burst=True)


class TestCommands(RQTestCase):
    def test_shutdown_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.connection
        worker = Worker('foo', connection=connection)

        p = Process(
            target=_send_shutdown_command, args=(worker.name, connection.connection_pool.connection_kwargs.copy())
        )
        p.start()
        worker.work()
        p.join(1)

    def test_pubsub_thread_survives_connection_error(self):
        """Ensure that the pubsub thread is still alive after its Redis connection is killed"""
        connection = self.connection
        worker = Worker('foo', connection=connection)
        worker.subscribe()

        assert worker.pubsub_thread.is_alive()

        # Kill the Redis connection
        for client in connection.client_list():
            try:
                connection.client_kill(client['addr'])
            except ResponseError:
                pass

        time.sleep(0.0)  # Allow other threads to run
        assert worker.pubsub_thread.is_alive()

    def test_pubsub_thread_exits_other_error(self):
        """Ensure that the pubsub thread  exits on other than redis.exceptions.ConnectionError"""
        connection = self.connection
        worker = Worker('foo', connection=connection)

        with mock.patch('redis.client.PubSub.get_message', new_callable=raise_exc_mock):
            worker.subscribe()

        worker.pubsub_thread.join()
        assert not worker.pubsub_thread.is_alive()

    def test_kill_horse_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.connection
        queue = Queue('foo', connection=connection)
        job = queue.enqueue(long_running_job, 4)
        worker = Worker('foo', connection=connection)

        p = Process(
            target=_send_kill_horse_command, args=(worker.name, connection.connection_pool.connection_kwargs.copy())
        )
        p.start()
        worker.work(burst=True)
        p.join(1)
        job.refresh()
        self.assertTrue(job.id in queue.failed_job_registry)

        p = Process(target=start_work, args=('foo', worker.name, connection.connection_pool.connection_kwargs.copy()))
        p.start()
        p.join(2)

        send_kill_horse_command(connection, worker.name)
        worker.refresh()
        # Since worker is not busy, command will be ignored
        self.assertEqual(worker.get_state(), WorkerStatus.IDLE)
        send_shutdown_command(connection, worker.name)

    def test_stop_job_command(self):
        """Ensure that stop_job command works properly."""

        connection = self.connection
        queue = Queue('foo', connection=connection, serializer=JSONSerializer)
        job = queue.enqueue(long_running_job, 3)
        worker = Worker('foo', connection=connection, serializer=JSONSerializer)

        # If job is not executing, an error is raised
        with self.assertRaises(InvalidJobOperation):
            send_stop_job_command(connection, job_id=job.id, serializer=JSONSerializer)

        # An exception is raised if job ID is invalid
        with self.assertRaises(NoSuchJobError):
            send_stop_job_command(connection, job_id='1', serializer=JSONSerializer)

        p = Process(
            target=start_work_burst, args=('foo', worker.name, connection.connection_pool.connection_kwargs.copy())
        )
        p.start()
        p.join(1)

        time.sleep(0.1)

        send_command(connection, worker.name, 'stop-job', job_id=1)
        time.sleep(0.25)
        # Worker still working due to job_id mismatch
        worker.refresh()
        self.assertEqual(worker.get_state(), WorkerStatus.BUSY)

        send_stop_job_command(connection, job_id=job.id, serializer=JSONSerializer)
        time.sleep(0.25)

        # Job status is set appropriately
        self.assertTrue(job.is_stopped)

        # Worker has stopped working
        worker.refresh()
        self.assertEqual(worker.get_state(), WorkerStatus.IDLE)
