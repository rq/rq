# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time
import ctypes
import socket
import signal

try:
    import _thread
except ImportError as ex:
    import thread as _thread


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
                                  'value ({0} seconds)'.format(self._timeout))

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


class WindowsDeathPenalty(BaseDeathPenalty):
    ws2_32 = ctypes.windll.ws2_32
    wsock32 = ctypes.windll.wsock32
    kernel32 = ctypes.windll.kernel32

    WSAIsBlocking = wsock32.WSAIsBlocking
    WSASetBlockingHook = ws2_32.WSASetBlockingHook
    WSAUnhookBlockingHook = ws2_32.WSAUnhookBlockingHook
    WSACancelBlockingCall = wsock32.WSACancelBlockingCall

    SetEvent = kernel32.SetEvent
    CloseHandle = kernel32.CloseHandle
    CreateEventW = kernel32.CreateEventW
    WaitForSingleObject = kernel32.WaitForSingleObject

    def thread_timeout_watchdog(self):
        while self.wait_for_timeout:
            current_time = time.time()

            if current_time >= self.time_limit:
                if current_time - self.time_limit > (self._timeout * 1.5):
                    # Print an error to report the forceful exit
                    os._exit(255)

                else:
                    with self.watchdog_lock:
                        self.timeout_detected = True
                        self.wait_for_timeout = False

                        _thread.interrupt_main()

            self.WaitForSingleObject(self.timeout_event, 500)

        return 0

    def socket_timeout_watchdog(self):
        current_time = time.time()

        if current_time >= self.time_limit:
            with self.watchdog_lock:
                blocking = self.WSAIsBlocking()

                if blocking:
                    self.SetEvent(self.timeout_event)
                    self.WSACancelBlockingCall()

        return 0

    def handle_death_penalty(self, signum, frame):
        if self.timeout_detected:
            raise JobTimeoutException('Job exceeded maximum timeout '
                                      'value ({0} seconds)'.format(self._timeout))

        else:
            self._original_sigint_handler(signum, frame)

    def setup_death_penalty(self):
        self.timeout_detected = False
        self.wait_for_timeout = True
        self.watchdog_lock = _thread.allocate_lock()
        self.timeout_event = self.CreateEventW(None, 1, 0, None)
        self.time_limit = time.time() + self._timeout

        # force winsock to be initialized

        dummy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        dummy_socket.close()

        # break free from blocking network calls if needed

        BLOCKING_HOOK = ctypes.WINFUNCTYPE(ctypes.wintypes.DWORD)

        self.watchdog_callback = BLOCKING_HOOK(self.socket_timeout_watchdog)

        self.WSASetBlockingHook(self.watchdog_callback)

        # trigger the signal if the execution time is exceeded

        self._original_sigint_handler = signal.getsignal(signal.SIGINT)

        signal.signal(signal.SIGINT, self.handle_death_penalty)

        self.watchdog_thread = _thread.start_new_thread(self.thread_timeout_watchdog, ())

    def cancel_death_penalty(self):
        self.wait_for_timeout = False

        self.WSAUnhookBlockingHook()
        self.CloseHandle(self.timeout_event)
