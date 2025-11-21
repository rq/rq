from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union

if TYPE_CHECKING:
    from _typeshed import ExcInfo
    from redis import Redis
    from typing_extensions import Unpack

    from .job import Dependency, Job


FunctionReferenceType = TypeVar('FunctionReferenceType', str, Callable[..., Any])
"""Custom type definition for what a `func` is in the context of a job.
A `func` can be a string with the function import path (eg.: `myfile.mymodule.myfunc`)
or a direct callable (function/method).
"""


JobDependencyType = Union['Dependency', 'Job', str, list[Union['Dependency', 'Job', str]]]

"""Custom type definition for a job dependencies.
A simple helper definition for the `depends_on` parameter when creating a job.
"""

SuccessCallbackType = Callable[['Job', 'Redis', Any], Any]
FailureCallbackType = Callable[['Job', 'Redis', Unpack[ExcInfo]], Any]
