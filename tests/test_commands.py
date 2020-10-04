import time

from multiprocessing import Process

from tests import RQTestCase
from tests.fixtures import long_running_job

from rq import Queue, Worker
from rq.command import send_command


class TestCommands(RQTestCase):

    def test_shutdown_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.testconn
        worker = Worker('foo', connection=connection)

        def send_shutdown_command():
            time.sleep(0.25)
            send_command(connection, worker.name, 'shutdown')

        p = Process(target=send_shutdown_command)
        p.start()
        worker.work()
        p.join(1)

    def test_kill_horse_command(self):
        """Ensure that shutdown command works properly."""
        connection = self.testconn
        queue = Queue('foo', connection=connection)
        job = queue.enqueue(long_running_job, 4)
        worker = Worker('foo', connection=connection)

        def send_kill_horse_command():
            """Waits 0.25 seconds before sending kill-horse command"""
            time.sleep(0.25)
            send_command(connection, worker.name, 'kill-horse')

        p = Process(target=send_kill_horse_command)
        p.start()
        worker.work(burst=True)
        p.join(1)
        job.refresh()
        self.assertTrue(job.id in queue.failed_job_registry)