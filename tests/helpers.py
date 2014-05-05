# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import timedelta


def strip_microseconds(date):
    return date - timedelta(microseconds=date.microsecond)
