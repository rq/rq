# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

from rq.utils import ColorizingStreamHandler
from rq.defaults import (DEFAULT_LOGGING_FORMAT,
                         DEFAULT_LOGGING_DATE_FORMAT)


def setup_loghandlers(level=None, date_format=DEFAULT_LOGGING_DATE_FORMAT,
                      log_format=DEFAULT_LOGGING_FORMAT):
    logger = logging.getLogger('rq.worker')

    if not _has_effective_handler(logger):
        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if level is not None:
        logger.setLevel(level)


def _has_effective_handler(logger):
    """
    Checks if a logger has a handler that will catch its messages in its logger hierarchy.
    :param `logging.Logger` logger: The logger to be checked.
    :return: True if a handler is found for the logger, False otherwise.
    :rtype: bool
    """
    while True:
        if logger.handlers:
            return True
        if not logger.parent:
            return False
        logger = logger.parent
