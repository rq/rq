# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

from rq.utils import ColorizingStreamHandler


def setup_loghandlers(level='INFO'):
    logger = logging.getLogger('rq.worker')
    if not logger.handlers:
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
