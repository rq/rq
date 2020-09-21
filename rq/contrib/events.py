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
"""
Events support for RQ workers and submitters
"""
import json
from datetime import datetime
from threading import Thread
from typing import ContextManager, Iterable, List, Optional, Set, Tuple

import attr
import cattr
from redis import Redis
from redis.client import PubSub
from rq.job import Job, JobStatus

from .utils.future import TypedFuture


@attr.s(auto_attribs=True)
class JobEvent:
    """
    Task event is a notification object that
    """

    id: str
    status: str
    date: datetime = attr.ib(factory=datetime.utcnow)


cattr.register_unstructure_hook(datetime, lambda v: v.isoformat())
cattr.register_structure_hook(datetime, lambda v, cl: datetime.fromisoformat(v))
# cattr.register_structure_hook_func(JobStatus, lambda v, cl: JobStatus(v))


class JobEventQueue(ContextManager):  # pylint: disable=inherit-non-class
    """
    Custom queue for job events.
    """

    # pylint: disable=super-init-not-called
    def __init__(self, redis, job_id, poll_frequency = 0.1, queue_prefix = None):
        self.redis = redis
        self.job_id = job_id
        self._name = ("{}-".format(queue_prefix) if queue_prefix else "") + "job-{}".format(job_id)

        self._pubsub = None
        self._requested_stop = False
        self._waiters: List[Tuple[TypedFuture[JobEvent], Optional[Set[JobStatus]]]] = []
        self._poll_frequency = poll_frequency
        self._wait_thread: Optional[Thread] = None

    @property
    def name(self) -> str:
        """
        :return:  Task queue name.
        """
        return self._name

    def send(self, event):
        """Send some JobEvent to redis."""
        self.redis.publish(self.name, json.dumps(cattr.unstructure(event)))

    def _receive_once(self, job, pubsub, old_status) -> Optional[JobEvent]:
        message = pubsub.get_message(ignore_subscribe_messages=True, timeout=self._poll_frequency)
        new_status: Optional[str] = None
        if old_status is not None and JobStatus.terminal(old_status):
            return JobEvent(job.id, status=old_status)
        if message is None:
            new_status = job.get_status()
        else:
            event: JobEvent = cattr.structure(json.loads(message["data"]), JobEvent)
            new_status = event.status  # pylint: disable=no-member
        if new_status is not None and (old_status is None or old_status != new_status):
            return JobEvent(job.id, status=new_status)
        return None

    def _wait(self):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.name)
        job = Job(id=self.job_id, connection=self.redis)
        old_status: Optional[str] = job.get_status()
        try:
            while not self._requested_stop:
                # acquire message using pubsub
                event = self._receive_once(job, pubsub, old_status)
                if event is not None:
                    to_delete = []
                    for idx, (waiter, only) in enumerate(reversed(self._waiters)):
                        if only is None or event.status in only:  # pylint: disable=no-member
                            waiter.set_result(event)
                            to_delete.append(idx)
                        elif JobStatus.terminal(event.status):  # pylint: disable=no-member
                            # pylint: disable=no-member
                            waiter.set_exception(RuntimeError("Terminal status: {}".format(event.status)))
                            to_delete.append(idx)
                    if len(to_delete) == len(self._waiters):
                        self._waiters.clear()
                    else:
                        for idx in to_delete:
                            del to_delete[idx]
        finally:
            for waiter, _ in self._waiters:
                waiter.set_exception(RuntimeError("JobQueue {} is closing.".format(self.job_id)))
            self._waiters.clear()
            pubsub.unsubscribe(self.name)

    @property
    def started(self) -> bool:
        """Check if this queue has started."""
        return not self._requested_stop and self._wait_thread and self._wait_thread.is_alive()

    def start_receiving(self):
        """Tell this queue to start recieving events."""
        if not self.started:
            self._wait_thread = Thread(target=self._wait, name="subscriber-{}".format(self.name))
            self._wait_thread.start()
            self._requested_stop = False

    def stop_receiving(self, wait_timeout: Optional[float] = None) -> bool:
        """Tell this queue to stop recieving events."""
        if self.started:
            self._requested_stop = True
            self._wait_thread.join(wait_timeout)
            self._wait_thread = None
        return not self.started

    @property
    def receiving(self) -> bool:
        """
        :return:  Check if the JobEventQueue is running and receiving events.
        """
        return self._wait_thread is not None and self._wait_thread.is_alive()

    def receive(self, *, only = None):
        """Recieve an event from this queue."""
        if not self.receiving:
            raise RuntimeError(
                "JobEventQueue is not receiving any events. "
                "Please start receiving events with start_receiving() or using 'with events'..."
            )
        fut = TypedFuture()
        self._waiters.append((fut, set(only) if only else None))
        return fut

    def wait(self):
        """Wait for queue to finish."""
        return self.receive(only=(status for status in JobStatus.values() if JobStatus.terminal(status)))

    def __enter__(self):
        self.start_receiving()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_receiving()
