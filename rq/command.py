import json
import os
import signal

from rq.exceptions import InvalidJobOperation
from rq.job import Job


PUBSUB_CHANNEL_TEMPLATE = 'rq:pubsub:%s'


def send_command(connection, worker_name, command, **kwargs):
    """Use connection' pubsub mechanism to send a command"""
    payload = {'command': command}
    if kwargs:
        payload.update(kwargs)
    connection.publish(PUBSUB_CHANNEL_TEMPLATE % worker_name, json.dumps(payload))


def parse_payload(payload):
    """Returns a dict of command data"""
    return json.loads(payload.get('data').decode())


def send_shutdown_command(connection, worker_name):
    """Send shutdown command"""
    send_command(connection, worker_name, 'shutdown')


def send_kill_horse_command(connection, worker_name):
    """Tell worker to kill it's horse"""
    send_command(connection, worker_name, 'kill-horse')


def send_stop_job_command(connection, job_id):
    """Instruct a worker to stop a job"""
    job = Job.fetch(job_id, connection=connection)
    if not job.worker_name:
        raise InvalidJobOperation('Job is not currently executing')
    send_command(connection, job.worker_name, 'stop-job', job_id=job_id)


def handle_command(worker, payload):
    """Parses payload and routes commands"""
    if payload['command'] == 'stop-job':
        handle_stop_job_command(worker, payload)
    elif payload['command'] == 'shutdown':
        handle_shutdown_command(worker)
    elif payload['command'] == 'kill-horse':
        handle_kill_worker_command(worker, payload)


def handle_shutdown_command(worker):
    """Perform shutdown command"""
    worker.log.info('Received shutdown command, sending SIGINT signal.')
    pid = os.getpid()
    os.kill(pid, signal.SIGINT)


def handle_kill_worker_command(worker, payload):
    """Stops work horse"""
    worker.log.info('Received kill horse command.')
    if worker.horse_pid:
        worker.log.info('Kiling horse...')
        worker.kill_horse()
    else:
        worker.log.info('Worker is not working, kill horse command ignored')


def handle_stop_job_command(worker, payload):
    """Handles stop job command"""
    job_id = payload.get('job_id')
    worker.log.debug('Received command to stop job %s', job_id)
    if job_id and worker.get_current_job_id() == job_id:
        worker.kill_horse()
    else:
        worker.log.info('Not working on job %s, command ignored.', job_id)
