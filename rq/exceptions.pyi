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
from typing import Any

class NoSuchJobError(Exception): ...
class InvalidJobDependency(Exception): ...
class InvalidJobOperationError(Exception): ...
class InvalidJobOperation(Exception): ...
class DequeueTimeout(Exception): ...

class ShutDownImminentException(Exception):
    extra_info: Any = ...
    def __init__(self, msg: Any, extra_info: Any) -> None: ...

class TimeoutFormatError(Exception): ...
