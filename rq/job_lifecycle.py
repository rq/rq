from __future__ import annotations

import traceback
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from .job import JobStatus

if TYPE_CHECKING:
    from redis.client import Pipeline

    from .job import Job


def call_exception_handlers(handlers: Iterable[Callable], job: Job, *exc_info) -> None:
    """Run each exception handler, stopping early if one returns an explicit falsy value.

    A `None` return means "continue" — only handlers that explicitly return a falsy value
    disable the remaining handlers in the chain.
    """
    for handler in handlers:
        should_continue = handler(job, *exc_info)
        if should_continue is None:
            should_continue = True
        if not should_continue:
            break


def format_exc_info(exc_info) -> str:
    """Format a (type, value, traceback) tuple into a string."""
    return ''.join(traceback.format_exception(*exc_info))


def record_job_failure(job: Job, exc_string: str, pipeline: Pipeline) -> None:
    """Set the job's status to FAILED and persist the failure (FailedJobRegistry + failure Result).

    This is the worker-less failure path — used by synchronous execution and abandoned-job
    cleanup. No worker is involved, so the failure Result is recorded with an empty worker name.
    """
    job.set_status(JobStatus.FAILED, pipeline=pipeline)
    job._handle_failure(exc_string, pipeline)
