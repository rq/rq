# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import warnings


def register_sentry(client, worker):
    """Given a Raven client and an RQ worker, registers exception handlers
    with the worker so exceptions are logged to Sentry.
    """
    def uses_supported_transport(url):
        supported_transports = set(['sync+', 'requests+'])
        return any(url.startswith(prefix) for prefix in supported_transports)

    if not any(uses_supported_transport(s) for s in client.servers):
        msg = ('Sentry error delivery is known to be unreliable when not '
               'delivered synchronously from RQ workers.  You are encouraged '
               'to change your DSN to use the sync+ or requests+ transport '
               'prefix.')
        warnings.warn(msg, UserWarning, stacklevel=2)

    def send_to_sentry(job, *exc_info):
        client.captureException(
            exc_info=exc_info,
            extra={
                'job_id': job.id,
                'func': job.func_name,
                'args': job.args,
                'kwargs': job.kwargs,
                'description': job.description,
            })

    worker.push_exc_handler(send_to_sentry)
