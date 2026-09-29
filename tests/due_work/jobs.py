"""The jobs the due-work contract runs: one that sends a message, with callbacks, and one whose service is down."""

from collections import Counter


class Outbox:
    """EXTERNAL SEAM: the service a job sends its message to, and what the callbacks announced."""

    def __init__(self) -> None:
        self.sent: Counter[str] = Counter()
        self.announced: list[str] = []
        self.failed_calls = 0

    def send(self, message: str) -> None:
        self.sent[message] += 1

    def clear(self) -> None:
        self.sent.clear()
        self.announced.clear()
        self.failed_calls = 0


outbox = Outbox()


def send_message(message: str) -> str:
    outbox.send(message)
    return message


def announce_success(job, connection, result) -> None:
    outbox.announced.append(f'sent {result!r}')


def announce_failure(job, connection, exc_type, exc_value, traceback) -> None:
    outbox.announced.append(f'failed: {exc_type.__name__}')


def call_down_service() -> None:
    outbox.failed_calls += 1
    raise ConnectionError('the service is down')
