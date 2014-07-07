# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

# Make sure that dictConfig is available
# This was added in Python 2.7/3.2
try:
    from logging.config import dictConfig
except ImportError:
    from rq.compat.dictconfig import dictConfig  # noqa

from rq.utils import ColorizingStreamHandler


def setup_loghandlers(level='INFO'):
    logger = logging.getLogger('rq.worker')
    if not logger.handlers:
        logger.setLevel(level)
        formatter = logging.Formatter(fmt='%(asctime)s %(message)s',
                                      datefmt='%H:%M:%S')
        handler = ColorizingStreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
