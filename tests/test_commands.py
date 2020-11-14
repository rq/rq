import time

from multiprocessing import Process

from tests import RQTestCase
from tests.fixtures import long_running_job

from rq import Queue, Worker
from rq.command import send_command, send_kill_horse_command, send_shutdown_command, send_stop_job_command
from rq.job import Job, JobStatus
from rq.worker import WorkerStatus


class TestCommands(RQTestCase):

    def test_shutdown_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.testconn
        worker = Worker('foo', connection=connection)

        def _send_shutdown_command():
            time.sleep(0.25)
            send_shutdown_command(connection, worker.name)

        p = Process(target=_send_shutdown_command)
        p.start()
        worker.work()
        p.join(1)

    def test_kill_horse_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.testconn
        queue = Queue('foo', connection=connection)
        job = queue.enqueue(long_running_job, 4)
        worker = Worker('foo', connection=connection)

        def _send_kill_horse_command():
            """Waits 0.25 seconds before sending kill-horse command"""
            time.sleep(0.25)
            send_kill_horse_command(connection, worker.name)

        p = Process(target=_send_kill_horse_command)
        p.start()
        worker.work(burst=True)
        p.join(1)
        job.refresh()
        self.assertTrue(job.id in queue.failed_job_registry)

    def test_stop_job_command(self):
        """Ensure that stop_job command works properly."""

        connection = self.testconn
        queue = Queue('foo', connection=connection)
        job = queue.enqueue(long_running_job, 3)
        worker = Worker('foo', connection=connection)

        def start_work():
            worker.work(burst=True)

        p = Process(target=start_work)
        p.start()
        p.join(1)

        time.sleep(0.1)
        send_stop_job_command(connection, worker.name, job_id='1')
        time.sleep(0.25)
        # Worker still working due to job_id mismatch
        worker.refresh()
        self.assertEqual(worker.get_state(), WorkerStatus.BUSY)

        send_stop_job_command(connection, worker.name, job_id=job.id)
        time.sleep(0.25)
        # Worker has stopped working        
        worker.refresh()
        self.assertEqual(worker.get_state(), WorkerStatus.IDLE)