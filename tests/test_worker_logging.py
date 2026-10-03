import logging
from unittest.mock import Mock

from rq import Worker


def test_worker_bootstrap_preserves_configured_log_level():
    """Worker bootstrap preserves configured log levels by default."""
    connection = Mock()
    connection.connection_pool.connection_kwargs = {}
    worker = Worker(['default'], connection=connection, prepare_for_work=False)
    worker.register_birth = Mock()
    worker.subscribe = Mock()
    worker.set_state = Mock()

    worker_logger = logging.getLogger('rq.worker')
    job_logger = logging.getLogger('rq.job')
    original_worker_level = worker_logger.level
    original_job_level = job_logger.level

    try:
        worker_logger.setLevel(logging.WARNING)
        job_logger.setLevel(logging.WARNING)

        worker.bootstrap()

        assert worker_logger.level == logging.WARNING
        assert job_logger.level == logging.WARNING
    finally:
        worker_logger.setLevel(original_worker_level)
        job_logger.setLevel(original_job_level)
