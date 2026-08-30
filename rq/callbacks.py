from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .defaults import CALLBACK_TIMEOUT
from .timeouts import JobTimeoutException
from .utils import parse_timeout, resolve_function_reference

if TYPE_CHECKING:
    from .job import Job
    from .timeouts import BaseDeathPenalty


class Callback:
    def __init__(self, func: str | Callable[..., Any], timeout: Any | None = None):
        if not isinstance(func, str) and not inspect.isfunction(func) and not inspect.isbuiltin(func):
            raise ValueError('Callback `func` must be a string or function')

        self.func = func
        self.timeout = parse_timeout(timeout) if timeout else CALLBACK_TIMEOUT

    @property
    def name(self) -> str:
        if isinstance(self.func, str):
            return self.func
        _, func_name = resolve_function_reference(self.func)
        return func_name


def execute_success_callback(job: Job, death_penalty_class: type[BaseDeathPenalty], result: Any) -> None:
    """Run the job's success callback under its timeout."""
    callback = job.success_callback
    if callback is None:
        return
    if inspect.iscoroutinefunction(callback):
        raise TypeError('Coroutine success callbacks are not supported')

    job.log.debug('Job %s: running success callback...', job.id)
    with death_penalty_class(job.success_callback_timeout, JobTimeoutException, job_id=job.id):
        callback(job, job.connection, result)


def execute_failure_callback(job: Job, death_penalty_class: type[BaseDeathPenalty], *exc_info) -> None:
    """Run the job's failure callback under its timeout."""
    callback = job.failure_callback
    if callback is None:
        return
    if inspect.iscoroutinefunction(callback):
        raise TypeError('Coroutine failure callbacks are not supported')

    job.log.debug('Job %s: running failure callback...', job.id)
    try:
        with death_penalty_class(job.failure_callback_timeout, JobTimeoutException, job_id=job.id):
            callback(job, job.connection, *exc_info)
    except Exception:
        job.log.exception('Job %s: error while executing failure callback', job.id)
        raise


def execute_stopped_callback(job: Job, death_penalty_class: type[BaseDeathPenalty]) -> None:
    """Run the job's stopped callback under its timeout."""
    callback = job.stopped_callback
    if callback is None:
        return
    if inspect.iscoroutinefunction(callback):
        raise TypeError('Coroutine stopped callbacks are not supported')

    job.log.debug('Job %s: running stopped callback...', job.id)
    try:
        with death_penalty_class(job.stopped_callback_timeout, JobTimeoutException, job_id=job.id):
            callback(job, job.connection)
    except Exception:
        job.log.exception('Job %s: error while executing stopped callback', job.id)
        raise
