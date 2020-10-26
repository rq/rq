import json


PUBSUB_CHANNEL_TEMPLATE = 'rq:pubsub:%s'


def send_command(redis, worker_name, command):
    """Use Redis' pubsub mechanism to send a command"""
    payload = {'command': command}
    redis.publish(PUBSUB_CHANNEL_TEMPLATE % worker_name, json.dumps(payload))


def parse_payload(payload):
    """Returns a dict of command data"""
    return json.loads(payload.get('data').decode())


def send_shutdown_command(redis, worker_name):
    """Send shutdown command"""
    send_command(redis, worker_name, 'shutdown')


def send_kill_horse_command(redis, worker_name):
    """Tell worker to kill it's horse"""
    send_command(redis, worker_name, 'kill-horse')
