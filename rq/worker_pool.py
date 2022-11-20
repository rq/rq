import logging
import os
import time
import traceback

from enum import Enum
from multiprocessing import Process
from typing import Dict, List, NamedTuple, Optional, Set, Union
from uuid import uuid4

from redis import Redis
from redis import SSLConnection, UnixDomainSocketConnection

from .queue import Queue
from .utils import parse_names
from .worker import Worker


class WorkerData(NamedTuple):
    name: str
    pid: int


def check_pid(pid: int) -> bool:
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


class Pool:

    class Status(Enum):
        STARTED = 1
        STOPPED = 2

    def __init__(self, queues: List[Union[str, Queue]], connection: Redis,
                 num_workers: int = 1, *args, **kwargs):
        self.num_workers: int = num_workers
        self._workers: List[Worker] = []
        self.log: logging.Logger = logging.getLogger(__name__)
        self._queue_names: Set[str] = set(parse_names(queues))
        self.connection = connection
        self.name: str = uuid4().hex
        self._burst: bool = True
        self._sleep: int = 0

        # A dictionary of WorkerData keyed by worker name
        self.worker_dict: Dict[str, WorkerData] = {}

        # Copy the connection kwargs before mutating them in order to not change the arguments
        # used by the current connection pool to create new connections
        self._connection_kwargs = connection.connection_pool.connection_kwargs.copy()
        # Redis does not accept parser_class argument which is sometimes present
        # on connection_pool kwargs, for example when hiredis is used
        self._connection_kwargs.pop('parser_class', None)
        self._connection_class = connection.__class__  # client
        connection_class = connection.connection_pool.connection_class
        if issubclass(connection_class, SSLConnection):
            self._connection_kwargs['ssl'] = True
        if issubclass(connection_class, UnixDomainSocketConnection):
            # The connection keyword arguments are obtained from
            # `UnixDomainSocketConnection`, which expects `path`, but passed to
            # `redis.client.Redis`, which expects `unix_socket_path`, renaming
            # the key is necessary.
            # `path` is not left in the dictionary as that keyword argument is
            # not expected by `redis.client.Redis` and would raise an exception.
            self._connection_kwargs['unix_socket_path'] = self._connection_kwargs.pop(
                'path'
            )

    @property
    def queues(self) -> List[Queue]:
        """Returns a list of Queue objects"""
        return [Queue(name, connection=self.connection) for name in self._queue_names]

    # def spawn_worker(self, queues: List[Queue], count: Optional[int] = None):
    #     """Instantiate a worker and adds it to worker list"""
    #     worker = Worker(self.queues, connection=self.connection)
    #     self._workers.append(worker)
    #     if count:
    #         self.log.debug(f'Spawned worker number {count}: {worker.name}')
    #     else:
    #         self.log.debug(f'Spawned worker {worker.name}')

    # def spawn_workers(self):
    #     """Instantiate workers"""
    #     self.log.info(f'Spawning {self.num_workers} workers')
    #     queues = self.queues
    #     for i in range(self.num_workers):
    #         self.spawn_worker(queues, i + 1)

    def reap_workers(self):
        """Removes dead workers from worker_dict"""
        self.log.debug('Reaping dead workers')
        worker_datas = list(self.worker_dict.values())

        for data in worker_datas:
            try:
                # To immediately detect that the worker is dead, we need to
                # call waitpid twice. The first waitpid() will return the
                os.waitpid(data.pid, os.WNOHANG)
                os.waitpid(data.pid, os.WNOHANG)
                self.log.debug(f'Worker {data.name} is alive')
            except ChildProcessError:
                self.log.debug(f'Worker {data.name} is dead')
                self.worker_dict.pop(data.name)

    def check_workers(self, respawn: bool = True) -> None:
        """
        Check whether workers are still alive
        """
        self.log.debug('Checking worker processes')
        self.reap_workers()
        # If we have less number of workers than num_workers,
        # respawn the difference
        if respawn:
            delta = self.num_workers - len(self.worker_dict)
            if delta:
                for i in range(delta):
                    self.start_worker(burst=self._burst, sleep =self._sleep)

    def start_worker(self, count: Optional[int] = None, burst: bool = True, sleep: int = 0):
        """
        Starts a worker and adds the data to worker_datas.
        * sleep: waits for X seconds before creating worker, for testing purposes
        """
        name = uuid4().hex
        process = Process(
            target=run_worker,
            args=(uuid4().hex, self._queue_names, self._connection_class, self._connection_kwargs),
            kwargs={'sleep': sleep, 'burst': burst},
            name=f'Worker {name} (Pool {self.name})'
        )
        process.start()
        worker_data = WorkerData(name=name, pid=process.pid)  # type: ignore
        self.worker_dict[name] = worker_data

        if count:
            self.log.debug(f'Spawned worker number {count}: {name}')
        else:
            self.log.debug(f'Spawned worker {name}')

    def start_workers(self, burst: bool = True, sleep: int = 0):
        """
        Run the workers
        * sleep: waits for X seconds before creating worker, for testing purposes
        """
        self.log.debug(f'Spawning {self.num_workers} workers')
        for i in range(self.num_workers):
            self.start_worker(i + 1, burst=burst, sleep=sleep)

    def start(self):
        self.log.debug(f'Starting worker pool {self.name}...')
        self.spawn_workers()

    def stop(self):
        pass


def run_worker(worker_name: str, queue_names: List[str],
               connection_class, connection_kwargs: dict, burst: bool = True, sleep: int = 0):
    connection = connection_class(**connection_kwargs)
    queues = [Queue(name, connection=connection) for name in queue_names]
    worker = Worker(queues, name=worker_name, connection=connection)
    worker.log.info("Starting worker started with PID %s", os.getpid())
    try:
        time.sleep(sleep)
        worker.work(burst=burst)
    except:  # noqa
        worker.log.error(
            'Worker [PID %s] raised an exception.\n%s',
            os.getpid(), traceback.format_exc()
        )
        raise
    worker.log.info("Worker with PID %s has stopped", os.getpid())
