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
from .connections import (
    Connection as Connection,
    get_current_connection as get_current_connection,
    pop_connection as pop_connection,
    push_connection as push_connection,
    use_connection as use_connection,
)
from .job import (
    Retry as Retry,
    cancel_job as cancel_job,
    get_current_job as get_current_job,
    requeue_job as requeue_job,
)
from .queue import Queue as Queue
from .version import VERSION
from .worker import SimpleWorker as SimpleWorker, Worker as Worker

__version__ = VERSION
