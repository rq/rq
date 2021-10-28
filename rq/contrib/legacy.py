# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


import logging
from rq.config import DEFAULT_CONFIG
from rq.utils import overwrite_config_connection


logger = logging.getLogger(__name__)


def cleanup_ghosts(conn=None, config=DEFAULT_CONFIG):
    """
    RQ versions < 0.3.6 suffered from a race condition where workers, when
    abruptly terminated, did not have a chance to clean up their worker
    registration, leading to reports of ghosted workers in `rqinfogit `.  Since
    0.3.6, new worker registrations automatically expire, and the worker will
    make sure to refresh the registrations as long as it's alive.

    This function will clean up any of such legacy ghosted workers.
    """
    config = overwrite_config_connection(config, conn)
    for worker in config.worker_class.all(config=config):
        if config.connection.ttl(worker.key) == -1:
            ttl = worker.default_worker_ttl
            config.connection.expire(worker.key, ttl)
            logger.info('Marked ghosted worker {0} to expire in {1} seconds.'.format(worker.name, ttl))
