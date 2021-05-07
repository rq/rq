# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import signal


class BaseTimeoutException(Exception):
    """Base exception for timeouts."""
    pass


class JobTimeoutException(BaseTimeoutException):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """
    pass


class HorseMonitorTimeoutException(BaseTimeoutException):
    """Raised when waiting for a horse exiting takes longer than the maximum
    timeout value.
    """
    pass


class BaseDeathPenalty:
    """Base class to setup job timeouts."""

    def __init__(self, timeout, exception=JobTimeoutException, **kwargs):
        self._timeout = timeout
        self._exception = exception

    def __enter__(self):
        self.setup_death_penalty()

    def __exit__(self, type, value, traceback):
        # Always cancel immediately, since we're done
        try:
            self.cancel_death_penalty()
        except BaseTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is
            # fired.  We may safely ignore this situation and consider the
            # body done.
            pass

        # __exit__ may return True to supress further exception handling.  We
        # don't want to suppress any exceptions here, since all errors should
        # just pass through, BaseTimeoutException being handled normally to the
        # invoking context.
        return False

    def setup_death_penalty(self):
        raise NotImplementedError()

    def cancel_death_penalty(self):
        raise NotImplementedError()


class UnixSignalDeathPenalty(BaseDeathPenalty):

    def handle_death_penalty(self, signum, frame):
        raise self._exception('Task exceeded maximum timeout value '
                              '({0} seconds)'.format(self._timeout))

    def setup_death_penalty(self):
        """Sets up an alarm signal and a signal handler that raises
        an exception after the timeout amount (expressed in seconds).
        """
        signal.signal(signal.SIGALRM, self.handle_death_penalty)
        signal.alarm(self._timeout)

    def cancel_death_penalty(self):
        """Removes the death penalty alarm and puts back the system into
        default signal handling.
        """
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
