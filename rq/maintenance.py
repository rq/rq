import warnings
from typing import TYPE_CHECKING

from .intermediate_queue import IntermediateQueue
from .queue import Queue

if TYPE_CHECKING:
    from .worker import BaseWorker


def clean_intermediate_queue(worker: 'BaseWorker', queue: Queue) -> None:
    """
    Check whether there are any jobs stuck in the intermediate queue.

    A job may be stuck in the intermediate queue if a worker has successfully dequeued a job
    but was not able to push it to the StartedJobRegistry. This may happen in rare cases
    of hardware or network failure.

    We consider a job to be stuck in the intermediate queue if it doesn't exist in the StartedJobRegistry.
    """
    warnings.warn(
        'clean_intermediate_queue is deprecated. Use IntermediateQueue.cleanup instead.',
        DeprecationWarning,
    )
    intermediate_queue = IntermediateQueue(queue.key, connection=queue.connection)
    intermediate_queue.cleanup(worker, queue)
