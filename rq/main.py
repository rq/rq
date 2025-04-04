import json
from logging import getLogger
from typing import List, Optional, Type, TypeVar

from redis import Redis

from rq import registry, utils
from rq.job import Job, JobStatus
from rq.queue import Queue
from rq.types import FunctionReferenceType
from rq.worker import Worker

QueueReference = TypeVar('QueueReference', str, Queue)
JobReference = TypeVar('JobReference', str, Job)
WorkerReference = TypeVar('WorkerReference', str, Worker)


class RQ:
    def __init__(
        self,
        queue: Optional[str] = None,
        connection: Optional[Redis] = None,
        queues: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        redis_url: Optional[str] = None,
        job_id_prefix: Optional[str] = None,
        worker_prefix: Optional[str] = None,
        serializer: Optional[str] = None,
        queue_class: Optional[str] = None,
        job_class: Optional[str] = None,
        worker_class: Optional[str] = None,
        log_level: Optional[str] = None,
        debug_mode: Optional[bool] = None,
        job_timeout: Optional[str] = None,
        result_ttl: Optional[str] = None,
        failure_ttl: Optional[str] = None,
        success_ttl: Optional[str] = None,
        maintenance_interval: Optional[str] = None,
        callback_timeout: Optional[str] = None,
        scheduler_fallback_interval: Optional[str] = None,
    ):
        self._queue = queue
        self._queues = queues
        self._namespace = namespace
        self._connection = connection
        self._redis_url = redis_url
        self._job_id_prefix = job_id_prefix
        self._worker_prefix = worker_prefix
        self._serializer = serializer
        self._queue_class = queue_class
        self._job_class = job_class
        self._worker_class = worker_class
        self._log_level = log_level
        self._debug_mode = debug_mode
        self._job_timeout = job_timeout
        self._result_ttl = result_ttl
        self._failure_ttl = failure_ttl
        self._success_ttl = success_ttl
        self._maintenance_interval = maintenance_interval
        self._callback_timeout = callback_timeout
        self._scheduler_fallback_interval = scheduler_fallback_interval
        self._log = getLogger(__name__)

    # Properties

    @property
    def queue_namespace(self) -> str:
        return "rq:queue:"

    @property
    def jobs_namespace(self) -> str:
        return "rq:job:"

    @property
    def workers_namespace(self) -> str:
        return "rq:worker:"

    @property
    def pubsub_channel(self) -> str:
        return "rq:pubsub:"

    @property
    def conn(self) -> Redis:
        """A safe way to access the connection object.

        Raises:
            ConnectionError: If no connection is passed.

        Returns:
            conn (Redis): The connection
        """
        if not self._connection:
            raise ConnectionError("No Redis connection was found")
        return self._connection

    # Sends

    def send(self, queue: QueueReference, f: 'FunctionReferenceType', *args, **kwargs) -> 'Job':
        """Creates a job to represent the delayed function call and enqueues it.
        Receives the same parameters accepted by the `enqueue_call` method.

        Args:
            f (FunctionReferenceType): The function reference
            args (*args): function args
            kwargs (*kwargs): function kargs

        Returns:
            job (Job): The created Job
        """
        _queue = self._get_queue_from_reference(queue)
        (
            f,
            timeout,
            description,
            result_ttl,
            ttl,
            failure_ttl,
            depends_on,
            job_id,
            at_front,
            meta,
            retry,
            on_success,
            on_failure,
            pipeline,
            args,
            kwargs,
        ) = _queue.parse_args(f, *args, **kwargs)

        return _queue.enqueue_call(
            func=f,
            args=args,
            kwargs=kwargs,
            timeout=timeout,
            result_ttl=result_ttl,
            ttl=ttl,
            failure_ttl=failure_ttl,
            description=description,
            depends_on=depends_on,
            job_id=job_id,
            at_front=at_front,
            meta=meta,
            retry=retry,
            on_success=on_success,
            on_failure=on_failure,
            pipeline=pipeline,
        )

    def send_job(self, queue: QueueReference, job: JobReference):
        pass

    def schedule(self):
        pass

    def schedule_job(self):
        pass

    def enqueue(self):
        pass

    def enqueue_at(self):
        pass

    def enqueue_in(self):
        pass

    # Queues

    def create_queue(self):
        pass

    def get_queue(self, name: str) -> Optional[Queue]:
        """Gets a key from it's name.
        Uses the `queue_namespace` for the lookup.

        Args:
            name (str): The queue name

        Returns:
            Optional[Queue]: The queue (if found)
        """
        queue_key = self.queue_namespace + name
        queue = Queue.from_queue_key(queue_key, connection=self.conn)
        return queue

    def get_all_queues(self, job_class: Optional[Type['Job']] = None, serializer=None) -> List['Queue']:
        """Returns an iterable of all Queues.

        Args:
            job_class (Optional[Job], optional): The Job class to use. Defaults to None.
            serializer (optional): The serializer to use. Defaults to None.

        Returns:
            queues (List[Queue]): A list of all queues.
        """

        def to_queue(queue_key):
            return Queue.from_queue_key(
                utils.as_text(queue_key), connection=self.conn, job_class=job_class, serializer=serializer
            )

        all_registerd_queues = self.conn.smembers(self.queue_namespace)
        all_queues = [to_queue(rq_key) for rq_key in all_registerd_queues if rq_key]
        return all_queues

    def get_queue_size(self, queue: QueueReference) -> int:
        """Returns the count of all jobs in the queue.

        Args:
            queue (QueueReference): The Queue, can be referenced by name or the Queue object itself.

        Returns:
            int: The queue count
        """
        _queue = self._get_queue_from_reference(queue)
        if _queue.connection is None:
            _queue.connection = self.conn
        return _queue.count

    def empty_queue(self, queue: QueueReference) -> bool:
        """Empties a specific queue.

        Args:
            queue (QueueReference): The queue reference

        Returns:
            bool: Whether the operation was successful.
        """
        _queue = self._get_queue_from_reference(queue)
        if _queue.connection is None:
            _queue.connection = self.conn
        try:
            _queue.empty()
        except Exception as e:
            return False
        return True

    # Workers

    def get_worker(
        self,
        name: str,
        job_class: Optional[Type[Job]] = None,
        queue_class: Optional[Type[Queue]] = None,
        serializer=None,
    ) -> Optional[Worker]:
        """Get's an existing Worker.

        Args:
            name (str): The Worker name
            job_class (Optional[Type[Job]], optional): The Job Class. Defaults to None.
            queue_class (Optional[Type[Queue]], optional): The Queue Class. Defaults to None.
            serializer (_type_, optional): The Serializer. Defaults to None.

        Returns:
            Worker: The Worker
        """
        worker_key = self._get_worker_key(name)
        if not self.conn.exists(worker_key):
            self.conn.srem(self.workers_namespace, worker_key)
            return None

        worker = Worker(
            [],
            name,
            connection=self.conn,
            job_class=job_class,  # type: ignore
            queue_class=queue_class,
            prepare_for_work=False,
            serializer=serializer,
        )
        worker.refresh()
        return worker

    def get_current_job(self, worker: WorkerReference) -> Optional[Job]:
        """Gets the current job being executed by a worker

        Args:
            worker (WorkerReference): The worker to get the current job from

        Returns:
            job (Optional[Job]): The job
        """
        _worker = self._get_worker_from_reference(worker)
        if not _worker:
            raise Exception("Worker not found")
        job = _worker.get_current_job()
        return job

    def get_current_job_id(self, worker: WorkerReference) -> Optional[str]:
        """Gets the current job id being executed by a worker

        Args:
            worker (WorkerReference): The worker to get the current job from

        Returns:
            job (Optional[Job]): The job
        """
        _worker = self._get_worker_from_reference(worker)
        if not _worker:
            raise Exception("Worker not found")
        job = _worker.get_current_job_id()
        return job

    def shutdown_worker(self, worker_name: str):
        """Sends a shutdown command to a worker.

        Args:
            worker_name (str): The worker name.
        """
        payload = {'command': 'shutdown'}
        channel = self.pubsub_channel + worker_name
        self.conn.publish(channel, json.dumps(payload))

    def reload_worker(self):
        pass

    def start_worker(self):
        pass

    # Job

    def get_job(self, job_id: str) -> Job:
        """Get a job from the server

        Args:
            job_id (str): The Job ID

        Returns:
            Job: The Job instance
        """
        job_key = self._get_job_key(job_id)
        job = Job(job_key.decode("utf-8"), connection=self.conn)
        job.refresh()
        return job

    def get_job_status(self, job_id: str) -> JobStatus:
        """Get's the status of a given job.

        Args:
            job_id (str): The job ID

        Returns:
            status (JobStatus): The job status
        """
        job_key = self._get_job_key(job_id)
        server_status = self.conn.hget(job_key, 'status')
        if not server_status:
            raise Exception(f"Job {job_id} not found")
        status = utils.as_text(server_status)
        return JobStatus(status)

    def get_all_jobs(self):
        pass

    def get_queued_jobs(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[Job]:
        """Get a list of queued jobs from a given queue reference

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): The offset. Defaults to 0.
            limit (int, optional): The limit. Defaults to -1 (no limit).

        Returns:
            List[Job]: The list of jobs
        """
        _queue = self._get_queue_from_reference(queue)
        jobs = _queue.get_jobs(offset, limit)
        return jobs

    def get_queued_jobs_ids(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of queued jobs ids from a given queue reference.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): The offset. Defaults to 0.
            limit (int, optional): The limit. Defaults to -1 (no limit).

        Returns:
            List[Job]: The list of jobs
        """
        _queue = self._get_queue_from_reference(queue)
        job_ids = _queue.get_job_ids(offset, limit)
        return job_ids

    def get_deferred_jobs_ids(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of deferred job ids from a given queue.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): Offset. Defaults to 0.
            limit (int, optional): Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of deferred job ids
        """
        job_ids = self._get_job_ids_from_registry(registry.DeferredJobRegistry, queue, offset, limit)
        return job_ids

    def get_scheduled_jobs(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of scheduled job ids from a given queue.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): Offset. Defaults to 0.
            limit (int, optional): Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of deferred job ids
        """
        job_ids = self._get_job_ids_from_registry(registry.ScheduledJobRegistry, queue, offset, limit)
        return job_ids

    def get_started_jobs(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of started job ids from a given queue.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): Offset. Defaults to 0.
            limit (int, optional): Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of deferred job ids
        """
        job_ids = self._get_job_ids_from_registry(registry.StartedJobRegistry, queue, offset, limit)
        return job_ids

    def get_failed_jobs(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of failed job ids from a given queue.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): Offset. Defaults to 0.
            limit (int, optional): Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of deferred job ids
        """
        job_ids = self._get_job_ids_from_registry(registry.FailedJobRegistry, queue, offset, limit)
        return job_ids

    def get_finished_jobs(self, queue: QueueReference, offset: int = 0, limit: int = -1) -> List[str]:
        """Get a list of finished job ids from a given queue.

        Args:
            queue (QueueReference): The queue reference
            offset (int, optional): Offset. Defaults to 0.
            limit (int, optional): Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of deferred job ids
        """
        job_ids = self._get_job_ids_from_registry(registry.FinishedJobRegistry, queue, offset, limit)
        return job_ids

    def job_exists(self, job_id: str) -> bool:
        """Checks whether a job exists.

        Args:
            job_id (str): The Job ID

        Returns:
            bool: True if exists, False otherwise.
        """
        job_key = self._get_job_key(job_id)
        job_exists = self.conn.exists(job_key)
        return bool(job_exists)

    def cancel_job(self):
        pass

    def exclude_job(self):
        pass

    def save_job(self):
        pass

    def create_job(self):
        pass

    ## Internals

    def _get_queue_from_reference(self, ref: QueueReference) -> Queue:
        """Get's a Queue instance from a reference.
        A Reference can be a string with the queue name, or the Queue instance itself.

        Args:
            ref (QueueReference): The Queue Reference

        Raises:
            TypeError: If any other type other than string or Queue is used

        Returns:
            Queue: The Queue Instance
        """
        if isinstance(ref, Queue):
            if not ref.connection and self._connection:
                ref.connection = self._connection
            return ref
        if not isinstance(ref, str):
            raise TypeError("Queue reference can only be a `str` or `Queue` type.")
        queue_key = self.queue_namespace + ref
        _queue = Queue.from_queue_key(queue_key, connection=self.conn)
        return _queue

    def _get_worker_from_reference(self, ref: WorkerReference) -> Optional[Worker]:
        """Get's a Worker instance from a reference.
        A Reference can be a string with the worker name, or the Worker instance itself.

        Args:
            ref (WorkerReference): The Worker Reference

        Raises:
            TypeError: If any other type other than string or Worker is used

        Returns:
            Worker: The Worker Instance
        """
        if isinstance(ref, Worker):
            if not ref.connection and self._connection:
                ref.connection = self._connection
            return ref
        if not isinstance(ref, str):
            raise TypeError("Worker reference can only be a `str` or `Worker` type.")
        _worker = self.get_worker(ref)
        return _worker

    def _get_job_ids_from_registry(
        self, registry: Type[registry.BaseRegistry], queue: QueueReference, offset: int = 0, limit: int = -1
    ) -> List[str]:
        """Get's a list of the Jobs IDs from a given registry.
        This is an internal shortcut to be used by many registries types.

        Args:
            registry (Type[registry.BaseRegistry]): The Registry
            queue (QueueReference): The Queue reference
            offset (int, optional): The Offset. Defaults to 0.
            limit (int, optional): The Limit. Defaults to -1 (no limit).

        Returns:
            List[str]: A list of jobs ids.
        """
        _queue = self._get_queue_from_reference(queue)
        _registry = registry(connection=self.conn, queue=_queue)
        job_ids = _registry.get_job_ids(offset, limit)
        return job_ids

    def _get_job_key(self, job_id: str) -> bytes:
        """Get's the Job Key from an ID.

        Args:
            job_id (str): The Job ID

        Returns:
            str: The Job Key
        """
        raw_key = self.jobs_namespace + job_id
        return raw_key.encode("utf-8")

    def _get_worker_key(self, worker_name: str) -> bytes:
        """Get's the Worker Key from a Name.

        Args:
            worker_name (str): The Worker Name

        Returns:
            str: The Worker Key
        """
        raw_key = self.workers_namespace + worker_name
        return raw_key.encode("utf-8")
