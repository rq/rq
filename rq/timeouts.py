import signal


class JobTimeoutException(Exception):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """
    pass


def interrupt_job_execution(signum, frame):
    raise JobTimeoutException('Job exceeded maximum timeout.')
