try:
    from logbook import Logger
except ImportError:
    from logging import Logger
from .worker import Worker

def run_daemon(queue_keys, rv_ttl=500, quit_when_done=False):
    """Simple implementation of a Redis queue worker, based on
    http://flask.pocoo.org/snippets/73/

    Will listen endlessly on the given queue keys.
    """
    worker = Worker(queue_keys, rv_ttl)

    log = Logger('worker')
    log.info('Listening for messages on Redis queues:')
    for key in queue_keys:
        log.info('- %s' % (key,))

    worker.work(quit_when_done)
