# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import signal


class JobTimeoutException(Exception):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """
    pass


class BaseDeathPenalty(object):
    """Base class to setup job timeouts."""

    def __init__(self, timeout):
        self._timeout = timeout

    def __enter__(self):
        self.setup_death_penalty()

    def __exit__(self, type, value, traceback):
        # Always cancel immediately, since we're done
        try:
            self.cancel_death_penalty()
        except JobTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is
            # fired.  We may safely ignore this situation and consider the
            # body done.
            pass

        # __exit__ may return True to supress further exception handling.  We
        # don't want to suppress any exceptions here, since all errors should
        # just pass through, JobTimeoutException being handled normally to the
        # invoking context.
        return False

    def setup_death_penalty(self):
        raise NotImplementedError()

    def cancel_death_penalty(self):
        raise NotImplementedError()


class UnixSignalDeathPenalty(BaseDeathPenalty):

    def handle_death_penalty(self, signum, frame):
        raise JobTimeoutException('Job exceeded maximum timeout '
                                  'value (%d seconds).' % self._timeout)

    def setup_death_penalty(self):
        """Sets up an alarm signal and a signal handler that raises
        a JobTimeoutException after the timeout amount (expressed in
        seconds).
        """
        signal.signal(signal.SIGALRM, self.handle_death_penalty)
        signal.alarm(self._timeout)

    def cancel_death_penalty(self):
        """Removes the death penalty alarm and puts back the system into
        default signal handling.
        """
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

# signal.SIGALRM is not supported under Windows, and RQ doesn't support
# Windows as a production platform, so the dummy class below
# gives a reasonable fallback for non-forked behavior when developing
# under Windows. See https://github.com/nvie/rq/issues/226
class NTDummySignalDeathPenalty(BaseDeathPenalty):

    def setup_death_penalty(self):
        """Dummy implemenation, do nothing"""

    def cancel_death_penalty(self):
        """Dummy implemenation, do nothing"""
