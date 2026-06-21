from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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
