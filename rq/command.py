import json
import os
import signal
import typing as t

if t.TYPE_CHECKING:
    from redis import Redis
    from .worker import Worker

from rq.exceptions import InvalidJobOperation
from rq.job import Job


PUBSUB_CHANNEL_TEMPLATE = 'rq:pubsub:%s'


def send_command(connection: 'Redis', worker_name: str, command, **kwargs):
    """
    Use connection' pubsub mechanism to send a command

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    payload = {'command': command}
    if kwargs:
        payload.update(kwargs)
    connection.publish(PUBSUB_CHANNEL_TEMPLATE % worker_name, json.dumps(payload))


def parse_payload(payload: t.Dict[t.Any, t.Any]) -> t.Dict[t.Any, t.Any]:
    """
    Returns a dict of command data

    Args:
        payload (dict): Parses the payload dict.
    """
    return json.loads(payload.get('data').decode())


def send_shutdown_command(connection: 'Redis', worker_name: str):
    """
    Sends a shutdown command to the pubsub topic.

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    send_command(connection, worker_name, 'shutdown')


def send_kill_horse_command(connection: 'Redis', worker_name: str):
    """
    Tell worker to kill it's horse

    Args:
        connection (Redis): A Redis Connection
        worker_name (str): The Job ID
    """
    send_command(connection, worker_name, 'kill-horse')


def send_stop_job_command(connection: 'Redis', job_id: str, serializer=None):
    """
    Instruct a worker to stop a job

    Args:
        connection (Redis): A Redis Connection
        job_id (str): The Job ID
        serializer (): The serializer
    """
    job = Job.fetch(job_id, connection=connection, serializer=serializer)
    if not job.worker_name:
        raise InvalidJobOperation('Job is not currently executing')
    send_command(connection, job.worker_name, 'stop-job', job_id=job_id)


def handle_command(worker: 'Worker', payload: t.Dict[t.Any, t.Any]):
    """Parses payload and routes commands

    Args:
        worker (Worker): The worker to use
        payload (t.Dict[t.Any, t.Any]): The Payload
    """
    if payload['command'] == 'stop-job':
        handle_stop_job_command(worker, payload)
    elif payload['command'] == 'shutdown':
        handle_shutdown_command(worker)
    elif payload['command'] == 'kill-horse':
        handle_kill_worker_command(worker, payload)


def handle_shutdown_command(worker: 'Worker'):
    """Perform shutdown command.

    Args:
        worker (Worker): The worker to use.
    """
    worker.log.info('Received shutdown command, sending SIGINT signal.')
    pid = os.getpid()
    os.kill(pid, signal.SIGINT)


def handle_kill_worker_command(worker: 'Worker', payload: t.Dict[t.Any, t.Any]):
    """
    Stops work horse

    Args:
        worker (Worker): The worker to stop
        payload (t.Dict[t.Any, t.Any]): The payload.
    """

    worker.log.info('Received kill horse command.')
    if worker.horse_pid:
        worker.log.info('Kiling horse...')
        worker.kill_horse()
    else:
        worker.log.info('Worker is not working, kill horse command ignored')


def handle_stop_job_command(worker: 'Worker', payload: t.Dict[t.Any, t.Any]):
    """Handles stop job command.

    Args:
        worker (Worker): The worker to use
        payload (t.Dict[t.Any, t.Any]): The payload.
    """
    job_id = payload.get('job_id')
    worker.log.debug('Received command to stop job %s', job_id)
    if job_id and worker.get_current_job_id() == job_id:
        # Sets the '_stopped_job_id' so that the job failure handler knows it
        # was intentional.
        worker._stopped_job_id = job_id
        worker.kill_horse()
    else:
        worker.log.info('Not working on job %s, command ignored.', job_id)
