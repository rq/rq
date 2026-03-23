from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, TypeVar

if TYPE_CHECKING:
    from typing import TypeAlias

    from redis import Redis

    from .job import Dependency, Job


FunctionReferenceType = TypeVar('FunctionReferenceType', str, Callable[..., Any])
"""Custom type definition for what a `func` is in the context of a job.
A `func` can be a string with the function import path (eg.: `myfile.mymodule.myfunc`)
or a direct callable (function/method).
"""


JobDependencyType: TypeAlias = 'Dependency | Job | str | list[Dependency | Job | str]'

"""Custom type definition for a job dependencies.
A simple helper definition for the `depends_on` parameter when creating a job.
"""

SuccessCallbackType = Callable[['Job', 'Redis', Any], Any]
FailureCallbackType = Callable[
    ['Job', 'Redis', Optional[type[BaseException]], Optional[BaseException], Optional[TracebackType]], Any
]
