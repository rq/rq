import threading
from unittest.mock import Mock, patch

import pytest
from redis import Redis
from redis.exceptions import ConnectionError

from rq import Worker


@pytest.mark.parametrize('error_type', [ConnectionError, KeyboardInterrupt])
def test_bootstrap_failure_stops_pubsub_thread(error_type):
    worker = Worker('default', connection=Redis(), prepare_for_work=False)
    stopped = threading.Event()
    thread = threading.Thread(target=stopped.wait, daemon=True)
    thread.stop = stopped.set
    pubsub = Mock()

    def run_in_thread(**kwargs):
        thread.start()
        return thread

    pubsub.run_in_thread.side_effect = run_in_thread
    error = error_type('startup failed')
    try:
        with (
            patch.object(worker, 'register_birth'),
            patch.object(worker.connection, 'pubsub', return_value=pubsub),
            patch.object(worker, 'set_state', side_effect=error),
        ):
            with pytest.raises(error_type) as exc_info:
                worker.bootstrap()
        assert exc_info.value is error
        assert not thread.is_alive()
        pubsub.close.assert_called_once()
    finally:
        stopped.set()
        if thread.ident is not None:
            thread.join(timeout=1)


@pytest.mark.parametrize('failure_point', ['subscribe', 'run_in_thread', 'set_state'])
def test_bootstrap_failure_closes_partial_subscription(failure_point):
    worker = Worker('default', connection=Redis(), prepare_for_work=False)
    pubsub = Mock()
    error = ConnectionError('startup failed')
    if failure_point != 'set_state':
        getattr(pubsub, failure_point).side_effect = error
    with (
        patch.object(worker, 'register_birth'),
        patch.object(worker.connection, 'pubsub', return_value=pubsub),
        patch.object(worker, 'set_state', side_effect=error if failure_point == 'set_state' else None),
    ):
        with pytest.raises(ConnectionError) as exc_info:
            worker.bootstrap()
    assert exc_info.value is error
    pubsub.close.assert_called_once()


def test_bootstrap_preserves_startup_error_when_unsubscribe_fails():
    worker = Worker('default', connection=Redis(), prepare_for_work=False)
    pubsub = Mock()
    pubsub.unsubscribe.side_effect = ConnectionError('unsubscribe failed')
    error = ConnectionError('startup failed')
    with (
        patch.object(worker, 'register_birth'),
        patch.object(worker.connection, 'pubsub', return_value=pubsub),
        patch.object(worker, 'set_state', side_effect=error),
    ):
        with pytest.raises(ConnectionError) as exc_info:
            worker.bootstrap()
    assert exc_info.value is error
    pubsub.run_in_thread.return_value.stop.assert_called_once()
    pubsub.run_in_thread.return_value.join.assert_called_once_with(timeout=1)
    pubsub.close.assert_called_once()


def test_successful_bootstrap_keeps_subscription_open():
    worker = Worker('default', connection=Redis(), prepare_for_work=False)
    pubsub = Mock()
    with (
        patch.object(worker, 'register_birth'),
        patch.object(worker.connection, 'pubsub', return_value=pubsub),
        patch.object(worker, 'set_state'),
    ):
        worker.bootstrap()
    pubsub.close.assert_not_called()
    pubsub.run_in_thread.return_value.stop.assert_not_called()
    worker.unsubscribe()
    pubsub.close.assert_called_once()
