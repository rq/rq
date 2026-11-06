import threading
import unittest
from unittest.mock import Mock, patch

from redis import Redis
from redis.exceptions import ConnectionError

from rq import Worker


class TestWorkerBootstrap(unittest.TestCase):
    def test_bootstrap_failure_stops_pubsub_thread(self):
        """A startup error stops the subscription thread and preserves the error."""
        for error_type in (ConnectionError, KeyboardInterrupt):
            with self.subTest(error_type=error_type):
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
                        with self.assertRaises(error_type) as exc_info:
                            worker.bootstrap()
                    self.assertIs(exc_info.exception, error)
                    self.assertFalse(thread.is_alive())
                    pubsub.close.assert_called_once()
                finally:
                    stopped.set()
                    if thread.ident is not None:
                        thread.join(timeout=1)

    def test_bootstrap_failure_closes_partial_subscription(self):
        """A failure at any subscription setup stage closes the pubsub object."""
        for failure_point in ('subscribe', 'run_in_thread', 'set_state'):
            with self.subTest(failure_point=failure_point):
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
                    with self.assertRaises(ConnectionError) as exc_info:
                        worker.bootstrap()
                self.assertIs(exc_info.exception, error)
                pubsub.close.assert_called_once()

    def test_bootstrap_preserves_startup_error_when_unsubscribe_fails(self):
        """An unsubscribe failure does not hide the startup error or skip cleanup."""
        worker = Worker('default', connection=Redis(), prepare_for_work=False)
        pubsub = Mock()
        pubsub.unsubscribe.side_effect = ConnectionError('unsubscribe failed')
        error = ConnectionError('startup failed')
        with (
            patch.object(worker, 'register_birth'),
            patch.object(worker.connection, 'pubsub', return_value=pubsub),
            patch.object(worker, 'set_state', side_effect=error),
        ):
            with self.assertRaises(ConnectionError) as exc_info:
                worker.bootstrap()
        self.assertIs(exc_info.exception, error)
        pubsub.run_in_thread.return_value.stop.assert_called_once()
        pubsub.run_in_thread.return_value.join.assert_called_once_with(timeout=1)
        pubsub.close.assert_called_once()

    def test_successful_bootstrap_keeps_subscription_open(self):
        """A successful startup keeps the subscription open until unsubscribed."""
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
