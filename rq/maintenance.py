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
    intermediate_queue = IntermediateQueue(queue.key, connection=queue.connection)
    job_ids = intermediate_queue.get_job_ids()

    for job_id in job_ids:
        job = queue.fetch_job(job_id)

        if job_id not in queue.started_job_registry:

            if not job:
                # If the job doesn't exist in the queue, we can safely remove it from the intermediate queue.
                intermediate_queue.remove(job_id)
                continue

            # If this is the first time we've seen this job, do nothing.
            # `set_first_seen` will return `True` if the key was set, `False` if it already existed.
            if intermediate_queue.set_first_seen(job_id):
                continue

            if intermediate_queue.should_be_cleaned_up(job_id):
                worker.handle_job_failure(job, queue, exc_string='Job was stuck in intermediate queue.')
                intermediate_queue.remove(job_id)
