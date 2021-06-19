# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)


class NoSuchJobError(Exception):
    pass


class DeserializationError(Exception):
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


class JobFailture(Exception):
    def __init__(self, msg, retry=None, seconds_until_next_retry=None, decrease_retries=True, show_traceback=True,
                 use_exc_handlers=True):
        super(JobFailture, self).__init__(msg)
        self.retry = retry
        self.seconds_until_next_retry = seconds_until_next_retry
        self.decrease_retries = decrease_retries
        self.show_traceback = show_traceback
        self.use_exc_handlers = use_exc_handlers
