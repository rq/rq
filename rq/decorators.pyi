# -*- coding: utf-8 -*-
#
# Copyright 2020 NVIDIA Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from redis import Redis

from .defaults import DEFAULT_RESULT_TTL as DEFAULT_RESULT_TTL
from .job import Job
from .queue import Queue as Queue
from .utils import backend_class as backend_class
from rq.compat import string_types as string_types
from typing import Any, Optional, Type, Iterable, Union, Callable


class job:
    queue_class: Type[Queue] = ...
    queue: str = ...
    connection: Redis = ...
    timeout: Optional[int] = ...
    result_ttl: Optional[int] = ...
    ttl: Optional[int] = ...
    meta: Optional[dict] = ...
    depends_on: Optional[Iterable[Union[str, Job]]] = ...
    at_front: Optional[bool] = ...
    description: Optional[str] = ...
    failure_ttl: Optional[int] = ...
    def __init__(
        self,
        queue: str,
        connection: Optional[Redis] = ...,
        timeout: Optional[int] = ...,
        result_ttl: int = ...,
        ttl: Optional[int] = ...,
        queue_class: Optional[Type[Queue]] = ...,
        depends_on: Optional[Iterable[Union[str, Job]]] = ...,
        at_front: Optional[bool] = ...,
        meta: Optional[dict] = ...,
        description: Optional[str] = ...,
        failure_ttl: Optional[int] = ...,
    ) -> None: ...
    def __call__(self, f: Callable) -> Job: ...
