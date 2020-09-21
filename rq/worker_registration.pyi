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
from redis.client import Pipeline

from .connections import Connection
from .queue import Queue
from .worker import Worker
from .compat import as_text as as_text
from typing import Any, Optional

WORKERS_BY_QUEUE_KEY: str
REDIS_WORKER_KEYS: str

def register(worker: Worker, pipeline: Optional[Pipeline] = ...) -> None: ...
def unregister(worker: Any, pipeline: Optional[Pipeline] = ...) -> None: ...
def get_keys(queue: Optional[Queue] = ..., connection: Optional[Connection] = ...): ...
def clean_worker_registry(queue: Queue) -> None: ...
