import json
import os
import signal
import warnings

from rq.exceptions import InvalidJobOperation
from rq.job import Job
from rq.config import DEFAULT_CONFIG, Config
from rq.utils import overwrite_config_connection


PUBSUB_CHANNEL_TEMPLATE = 'rq:pubsub:%s'


def send_command(worker_name, command, config=DEFAULT_CONFIG, **kwargs):
    """Use connection' pubsub mechanism to send a command"""
    payload = {'command': command}
    if kwargs:
        payload.update(kwargs)
    config.connection.publish(PUBSUB_CHANNEL_TEMPLATE % worker_name, json.dumps(payload))


def parse_payload(payload):
    """Returns a dict of command data"""
    return json.loads(payload.get('data').decode())


def send_shutdown_command(worker_name, config=DEFAULT_CONFIG, connection=None):
    """Send shutdown command"""
    config = overwrite_config_connection(config, connection)
    send_command(worker_name, 'shutdown', config=config)


def send_kill_horse_command(worker_name, config=DEFAULT_CONFIG, connection=None):
    """Tell worker to kill it's horse"""
    config = overwrite_config_connection(config, connection)
    send_command(worker_name, 'kill-horse', config=config)


def send_stop_job_command(job_id, serializer=None, config=DEFAULT_CONFIG, connection=None):
    """Instruct a worker to stop a job"""
    if serializer is not None:
        warnings.warn('serializer argument of send_stop_job_command is deprecated and will be removed in RQ 2. '
                      'Use send_stop_job_command(config=Config(serializer=serializer)) instead.', DeprecationWarning)
        config = Config(template=config, serializer=serializer)
    config = overwrite_config_connection(config, connection)
    job = Job.fetch(job_id, config=config)
    if not job.worker_name:
        raise InvalidJobOperation('Job is not currently executing')
    send_command(job.worker_name, 'stop-job', config=config, job_id=job_id)


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
    cid = worker.get_current_job_id()
    print(cid)
    if job_id and cid == job_id:
        # Sets the '_stopped_job_id' so that the job failure handler knows it
        # was intentional.
        worker._stopped_job_id = job_id
        worker.kill_horse()
    else:
        worker.log.info('Not working on job %s, command ignored.', job_id)
