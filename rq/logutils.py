# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

from rq.utils import ColorizingStreamHandler


def setup_loghandlers(level):
    logger = logging.getLogger('rq.worker')
    if not _has_effective_handler(logger):
        logger.setLevel(level)
        # This statement doesn't set level properly in Python-2.6
        # Following is an additional check to see if level has been set to
        # appropriate(int) value
        if logger.getEffectiveLevel() == level:
            # Python-2.6. Set again by using logging.INFO etc.
            level_int = getattr(logging, level)
            logger.setLevel(level_int)
        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                      datefmt='%H:%M:%S')
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)


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
