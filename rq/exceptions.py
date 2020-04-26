# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


class NoSuchJobError(Exception):
    pass


class InvalidJobDependency(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class InvalidJobOperation(Exception):
    pass


class DequeueTimeout(Exception):
    pass


class ShutDownImminentException(Exception):
    def __init__(self, msg, extra_info):
        self.extra_info = extra_info
        super(ShutDownImminentException, self).__init__(msg)


class TimeoutFormatError(Exception):
    pass
