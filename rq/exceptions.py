# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


class NoSuchJobError(Exception):
    pass


class InvalidJobDependency(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class UnpickleError(Exception):
    def __init__(self, message, raw_data, inner_exception=None):
        super(UnpickleError, self).__init__(message, inner_exception)
        self.raw_data = raw_data


class DequeueTimeout(Exception):
    pass


class ShutDownImminentException(BaseException):
    def __init__(self, msg=None, extra_info=None):
        self.extra_info = extra_info
        super(ShutDownImminentException, self).__init__(msg)


class TimeoutFormatError(Exception):
    pass
