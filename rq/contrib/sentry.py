def register_sentry(client, worker):
    """Given a Raven client and an RQ worker, registers exception handlers
    with the worker so exceptions are logged to Sentry.
    """
    def send_to_sentry(job, *exc_info):
        client.captureException(
                exc_info=exc_info,
                extra={
                    'job_id': job.id,
                    'func': job.func,
                    'args': job.args,
                    'kwargs': job.kwargs,
                    })

    worker.push_exc_handler(send_to_sentry)
