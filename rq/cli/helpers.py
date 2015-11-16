# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
import importlib
import time
from functools import partial

import click
import redis
from redis import StrictRedis
from redis.sentinel import Sentinel
from rq.defaults import (DEFAULT_CONNECTION_CLASS, DEFAULT_JOB_CLASS,
                         DEFAULT_QUEUE_CLASS, DEFAULT_WORKER_CLASS)
from rq.logutils import setup_loghandlers
from rq.utils import import_attribute
from rq.worker import WorkerStatus

red = partial(click.style, fg='red')
green = partial(click.style, fg='green')
yellow = partial(click.style, fg='yellow')


def read_config_file(module):
    """Reads all UPPERCASE variables defined in the given module file."""
    settings = importlib.import_module(module)
    return dict([(k, v)
                 for k, v in settings.__dict__.items()
                 if k.upper() == k])


def get_redis_from_config(settings, connection_class=StrictRedis):
    """Returns a StrictRedis instance from a dictionary of settings.
       To use redis sentinel, you must specify a dictionary in the configuration file.
       Example of a dictionary with keys without values:
       SENTINEL: {'INSTANCES':, 'SOCKET_TIMEOUT':, 'PASSWORD':,'DB':, 'MASTER_NAME':}
    """
    if settings.get('REDIS_URL') is not None:
        return connection_class.from_url(settings['REDIS_URL'])

    elif settings.get('SENTINEL') is not None:
        instances = settings['SENTINEL'].get('INSTANCES', [('localhost', 26379)])
        socket_timeout = settings['SENTINEL'].get('SOCKET_TIMEOUT', None)
        password = settings['SENTINEL'].get('PASSWORD', None)
        db = settings['SENTINEL'].get('DB', 0)
        master_name = settings['SENTINEL'].get('MASTER_NAME', 'mymaster')
        sn = Sentinel(instances, socket_timeout=socket_timeout, password=password, db=db)
        return sn.master_for(master_name)

    kwargs = {
        'host': settings.get('REDIS_HOST', 'localhost'),
        'port': settings.get('REDIS_PORT', 6379),
        'db': settings.get('REDIS_DB', 0),
        'password': settings.get('REDIS_PASSWORD', None),
    }

    use_ssl = settings.get('REDIS_SSL', False)
    if use_ssl:
        # If SSL is required, we need to depend on redis-py being 2.10 at
        # least
        def safeint(x):
            try:
                return int(x)
            except ValueError:
                return 0

        version_info = tuple(safeint(x) for x in redis.__version__.split('.'))
        if not version_info >= (2, 10):
            raise RuntimeError('Using SSL requires a redis-py version >= 2.10')
        kwargs['ssl'] = use_ssl
    return connection_class(**kwargs)


def pad(s, pad_to_length):
    """Pads the given string to the given length."""
    return ('%-' + '%ds' % pad_to_length) % (s,)


def get_scale(x):
    """Finds the lowest scale where x <= scale."""
    scales = [20, 50, 100, 200, 400, 600, 800, 1000]
    for scale in scales:
        if x <= scale:
            return scale
    return x


def state_symbol(state):
    symbols = {
        WorkerStatus.BUSY: red('busy'),
        WorkerStatus.IDLE: green('idle'),
        WorkerStatus.SUSPENDED: yellow('suspended'),
    }
    try:
        return symbols[state]
    except KeyError:
        return state


def show_queues(queues, raw, by_queue, queue_class, worker_class):
    if queues:
        qs = list(map(queue_class, queues))
    else:
        qs = queue_class.all()

    num_jobs = 0
    termwidth, _ = click.get_terminal_size()
    chartwidth = min(20, termwidth - 20)

    max_count = 0
    counts = dict()
    for q in qs:
        count = q.count
        counts[q] = count
        max_count = max(max_count, count)
    scale = get_scale(max_count)
    ratio = chartwidth * 1.0 / scale

    for q in qs:
        count = counts[q]
        if not raw:
            chart = green('|' + '█' * int(ratio * count))
            line = '%-12s %s %d' % (q.name, chart, count)
        else:
            line = 'queue %s %d' % (q.name, count)
        click.echo(line)

        num_jobs += count

    # print summary when not in raw mode
    if not raw:
        click.echo('%d queues, %d jobs total' % (len(qs), num_jobs))


def show_workers(queues, raw, by_queue, queue_class, worker_class):
    if queues:
        qs = list(map(queue_class, queues))

        def any_matching_queue(worker):
            def queue_matches(q):
                return q in qs
            return any(map(queue_matches, worker.queues))

        # Filter out workers that don't match the queue filter
        ws = [w for w in worker_class.all() if any_matching_queue(w)]

        def filter_queues(queue_names):
            return [qname for qname in queue_names if queue_class(qname) in qs]

    else:
        qs = queue_class.all()
        ws = worker_class.all()
        filter_queues = (lambda x: x)

    if not by_queue:
        for w in ws:
            worker_queues = filter_queues(w.queue_names())
            if not raw:
                click.echo('%s %s: %s' % (w.name, state_symbol(w.get_state()), ', '.join(worker_queues)))
            else:
                click.echo('worker %s %s %s' % (w.name, w.get_state(), ','.join(worker_queues)))
    else:
        # Create reverse lookup table
        queues = dict([(q, []) for q in qs])
        for w in ws:
            for q in w.queues:
                if q not in queues:
                    continue
                queues[q].append(w)

        max_qname = max(map(lambda q: len(q.name), queues.keys())) if queues else 0
        for q in queues:
            if queues[q]:
                queues_str = ", ".join(sorted(map(lambda w: '%s (%s)' % (w.name, state_symbol(w.get_state())), queues[q])))  # noqa
            else:
                queues_str = '–'
            click.echo('%s %s' % (pad(q.name + ':', max_qname + 1), queues_str))

    if not raw:
        click.echo('%d workers, %d queues' % (len(ws), len(qs)))


def show_both(queues, raw, by_queue, queue_class, worker_class):
    show_queues(queues, raw, by_queue, queue_class, worker_class)
    if not raw:
        click.echo('')
    show_workers(queues, raw, by_queue, queue_class, worker_class)
    if not raw:
        click.echo('')
        import datetime
        click.echo('Updated: %s' % datetime.datetime.now())


def refresh(interval, func, *args):
    while True:
        if interval:
            click.clear()
        func(*args)
        if interval:
            time.sleep(interval)
        else:
            break


def setup_loghandlers_from_args(verbose, quiet):
    if verbose and quiet:
        raise RuntimeError("Flags --verbose and --quiet are mutually exclusive.")

    if verbose:
        level = 'DEBUG'
    elif quiet:
        level = 'WARNING'
    else:
        level = 'INFO'
    setup_loghandlers(level)


class CliConfig(object):
    """A helper class to be used with click commands, to handle shared options"""
    def __init__(self, url=None, config=None, worker_class=DEFAULT_WORKER_CLASS,
                 job_class=DEFAULT_JOB_CLASS, queue_class=DEFAULT_QUEUE_CLASS,
                 connection_class=DEFAULT_CONNECTION_CLASS, path=None, *args, **kwargs):
        self._connection = None
        self.url = url
        self.config = config

        if path:
            for pth in path:
                sys.path.append(pth)

        try:
            self.worker_class = import_attribute(worker_class)
        except (ImportError, AttributeError) as exc:
            raise click.BadParameter(str(exc), param_hint='--worker-class')
        try:
            self.job_class = import_attribute(job_class)
        except (ImportError, AttributeError) as exc:
            raise click.BadParameter(str(exc), param_hint='--job-class')

        try:
            self.queue_class = import_attribute(queue_class)
        except (ImportError, AttributeError) as exc:
            raise click.BadParameter(str(exc), param_hint='--queue-class')

        try:
            self.connection_class = import_attribute(connection_class)
        except (ImportError, AttributeError) as exc:
            raise click.BadParameter(str(exc), param_hint='--connection-class')

    @property
    def connection(self):
        if self._connection is None:
            if self.url:
                self._connection = self.connection_class.from_url(self.url)
            else:
                settings = read_config_file(self.config) if self.config else {}
                self._connection = get_redis_from_config(settings,
                                                         self.connection_class)
        return self._connection
