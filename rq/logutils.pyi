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
from rq.defaults import DEFAULT_LOGGING_DATE_FORMAT as DEFAULT_LOGGING_DATE_FORMAT, DEFAULT_LOGGING_FORMAT as DEFAULT_LOGGING_FORMAT
from rq.utils import ColorizingStreamHandler as ColorizingStreamHandler
from typing import Any, Optional

def setup_loghandlers(level: Optional[Any] = ..., date_format: Any = ..., log_format: Any = ..., name: str = ...) -> None: ...
def _has_effective_handler(logger: Any): ...
