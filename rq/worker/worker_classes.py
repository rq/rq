import contextlib
import errno
import os
import signal
import sys
import time
from random import shuffle
from typing import TYPE_CHECKING, Optional

from ..defaults import DEFAULT_WORKER_TTL
from ..exceptions import InvalidJobOperation, ShutDownImminentException
from ..job import Job, JobStatus
from ..timeouts import HorseMonitorTimeoutException
from ..utils import now
from .base import SHUTDOWN_SIGNAL, BaseWorker, WorkerStatus, signal_name

if TYPE_CHECKING:
    from ..queue import Queue

    try:
        from resource import struct_rusage
    except ImportError:
        pass


class Worker(BaseWorker):
    def kill_horse(self, sig: signal.Signals = SHUTDOWN_SIGNAL):
        """Kill the horse but catch "No such process" error has the horse could already be dead.

        Args:
            sig (signal.Signals, optional): _description_. Defaults to SIGKILL.
        """
        try:
            os.killpg(os.getpgid(self.horse_pid), sig)
            self.log.info('Worker %s: killed horse pid %s', self.name, self.horse_pid)
        except OSError as e:
            if e.errno == errno.ESRCH:
                # "No such process" is fine with us
                self.log.debug('Worker %s: horse already dead', self.name)
            else:
                raise

    def wait_for_horse(self) -> tuple[Optional[int], Optional[int], Optional['struct_rusage']]:
        """Waits for the horse process to complete.
        Uses `0` as argument as to include "any child in the process group of the current process".
        """
        pid = stat = rusage = None
        with contextlib.suppress(ChildProcessError):  # ChildProcessError: [Errno 10] No child processes
            pid, stat, rusage = os.wait4(self.horse_pid, 0)
        return pid, stat, rusage

    def fork_work_horse(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        This is where the `fork()` actually happens.

        Args:
            job (Job): The Job that will be ran
            queue (Queue): The queue
        """
        child_pid = os.fork()
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        if child_pid == 0:
            os.setpgrp()
            self.main_work_horse(job, queue)
            os._exit(0)  # just in case
        else:
            self._horse_pid = child_pid
            self.procline(f'Forked {child_pid} at {time.time()}')

    def monitor_work_horse(self, job: 'Job', queue: 'Queue'):
        """The worker will monitor the work horse and make sure that it
        either executes successfully or the status of the job is set to
        failed

        Args:
            job (Job): _description_
            queue (Queue): _description_
        """
        retpid = ret_val = rusage = None
        job.started_at = now()
        while True:
            try:
                with self.death_penalty_class(self.job_monitoring_interval, HorseMonitorTimeoutException):
                    retpid, ret_val, rusage = self.wait_for_horse()
                break
            except HorseMonitorTimeoutException:
                # Horse has not exited yet and is still running.
                # Send a heartbeat to keep the worker alive.
                self.set_current_job_working_time((now() - job.started_at).total_seconds())

                # Kill the job from this side if something is really wrong (interpreter lock/etc).
                if job.timeout != -1 and self.current_job_working_time > (job.timeout + 60):  # type: ignore
                    self.heartbeat(self.job_monitoring_interval + 60)
                    self.kill_horse()
                    self.wait_for_horse()
                    break

                self.maintain_heartbeats(job)

            except OSError as e:
                # In case we encountered an OSError due to EINTR (which is
                # caused by a SIGINT or SIGTERM signal during
                # os.waitpid()), we simply ignore it and enter the next
                # iteration of the loop, waiting for the child to end.  In
                # any other case, this is some other unexpected OS error,
                # which we don't want to catch, so we re-raise those ones.
                if e.errno != errno.EINTR:
                    raise
                # Send a heartbeat to keep the worker alive.
                self.heartbeat()

        self.set_current_job_working_time(0)
        self._horse_pid = 0  # Set horse PID to 0, horse has finished working

        self.log.debug(
            'Worker %s: work horse finished for job %s: retpid=%s, ret_val=%s', self.name, job.id, retpid, ret_val
        )

        if ret_val == os.EX_OK:  # The process exited normally.
            return

        try:
            job_status = job.get_status()
        except InvalidJobOperation:
            return  # Job completed and its ttl has expired

        if self._stopped_job_id == job.id:
            # Work-horse killed deliberately
            self.log.warning('Worker %s: job %s stopped by user, moving job to FailedJobRegistry', self.name, job.id)
            if job.stopped_callback:
                job.execute_stopped_callback(self.death_penalty_class)
            self.handle_job_failure(job, queue=queue, exc_string='Job stopped by user, work-horse terminated.')
        elif job_status not in [JobStatus.FINISHED, JobStatus.FAILED]:
            if not job.ended_at:
                job.ended_at = now()

            # Unhandled failure: move the job to the failed queue
            signal_msg = f' (signal {os.WTERMSIG(ret_val)})' if ret_val and os.WIFSIGNALED(ret_val) else ''
            exc_string = f'Work-horse terminated unexpectedly; waitpid returned {ret_val}{signal_msg}; '
            self.log.warning('Worker %s: moving job %s to FailedJobRegistry (%s)', self.name, job.id, exc_string)

            self.handle_work_horse_killed(job, retpid, ret_val, rusage)
            self.handle_job_failure(job, queue=queue, exc_string=exc_string)

    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work and passes it a job.
        The worker will wait for the work horse and make sure it executes
        within the given timeout bounds, or will end the work horse with
        SIGALRM.
        """
        self.prepare_execution(job)
        self.fork_work_horse(job, queue)
        self.monitor_work_horse(job, queue)
        self.set_state(WorkerStatus.IDLE)


class SpawnWorker(Worker):
    """Worker implementation that uses os.spawn() instead of os.fork().
    This implementation is intended for environments where `os.fork()` is not available.
    """

    def fork_work_horse(self, job: 'Job', queue: 'Queue'):
        """Spawns a work horse to perform the actual work using os.spawn()."""
        os.environ['RQ_WORKER_ID'] = self.name
        os.environ['RQ_JOB_ID'] = job.id
        os.environ['RQ_EXECUTION_ID'] = self.execution.id  # type: ignore

        redis_kwargs = self.connection.connection_pool.connection_kwargs
        if redis_kwargs.get('retry'):
            # Remove retry from connection kwargs to avoid issues with os.spawnv
            del redis_kwargs['retry']

        child_pid = os.spawnv(
            os.P_NOWAIT,
            sys.executable,
            [
                sys.executable,
                '-c',
                f"""
import os
import sys
from redis import Redis
from rq import Worker, Queue
from rq.job import Job
from rq.executions import Execution

# Recreate worker instance
redis = Redis(**{redis_kwargs})
worker = Worker.find_by_key("{self.key}", connection=redis)
if not worker:
    sys.exit(1)

# Reconstruct job, queue and execution objects
job = Job.fetch("{job.id}", connection=worker.connection)
queue = Queue("{queue.name}", connection=worker.connection)
execution_id = os.environ.get('RQ_EXECUTION_ID')
worker.execution = Execution.fetch(execution_id, job.id, connection=worker.connection)

# Set up work horse
os.setpgrp()
worker._is_horse = True
worker.main_work_horse(job, queue)
""",
            ],
        )

        self._horse_pid = child_pid
        self.procline(f'Spawned {child_pid} at {time.time()}')


class SimpleWorker(BaseWorker):
    def execute_job(self, job: 'Job', queue: 'Queue'):
        """Execute job in same thread/process, do not fork()"""
        self.prepare_execution(job)
        self.perform_job(job, queue)
        self.set_state(WorkerStatus.IDLE)

    def get_heartbeat_ttl(self, job: 'Job') -> int:
        """-1" means that jobs never timeout. In this case, we should _not_ do -1 + 60 = 59.
        We should just stick to DEFAULT_WORKER_TTL.

        Args:
            job (Job): The Job

        Returns:
            ttl (int): TTL
        """
        if job.timeout == -1:
            return DEFAULT_WORKER_TTL
        else:
            return int(job.timeout or DEFAULT_WORKER_TTL) + 60


class HerokuWorker(Worker):
    """
    Modified version of rq worker which:
    * stops work horses getting killed with SIGTERM
    * sends SIGRTMIN to work horses on SIGTERM to the main process which in turn
    causes the horse to crash `imminent_shutdown_delay` seconds later
    """

    imminent_shutdown_delay = 6
    frame_properties = ['f_code', 'f_lasti', 'f_lineno', 'f_locals', 'f_trace']

    def setup_work_horse_signals(self):
        """Modified to ignore SIGINT and SIGTERM and only handle SIGRTMIN"""
        signal.signal(signal.SIGRTMIN, self.request_stop_sigrtmin)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_warm_shutdown_request(self):
        """If horse is alive send it SIGRTMIN"""
        if self.horse_pid != 0:
            self.log.info('Worker %s: warm shut down requested, sending horse SIGRTMIN signal', self.key)
            self.kill_horse(sig=signal.SIGRTMIN)
        else:
            self.log.warning('Warm shut down requested, no horse found')

    def request_stop_sigrtmin(self, signum, frame):
        if self.imminent_shutdown_delay == 0:
            self.log.warning('Imminent shutdown, raising ShutDownImminentException immediately')
            self.request_force_stop_sigrtmin(signum, frame)
        else:
            self.log.warning(
                'Imminent shutdown, raising ShutDownImminentException in %d seconds', self.imminent_shutdown_delay
            )
            signal.signal(signal.SIGRTMIN, self.request_force_stop_sigrtmin)
            signal.signal(signal.SIGALRM, self.request_force_stop_sigrtmin)
            signal.alarm(self.imminent_shutdown_delay)

    def request_force_stop_sigrtmin(self, signum, frame):
        info = {attr: getattr(frame, attr) for attr in self.frame_properties}
        self.log.warning('raising ShutDownImminentException to cancel job...')
        raise ShutDownImminentException(f'shut down imminent (signal: {signal_name(signum)})', info)


class RoundRobinWorker(Worker):
    """
    Modified version of Worker that dequeues jobs from the queues using a round-robin strategy.
    """

    def reorder_queues(self, reference_queue):
        pos = self._ordered_queues.index(reference_queue)
        self._ordered_queues = self._ordered_queues[pos + 1 :] + self._ordered_queues[: pos + 1]


class RandomWorker(Worker):
    """
    Modified version of Worker that dequeues jobs from the queues using a random strategy.
    """

    def reorder_queues(self, reference_queue):
        shuffle(self._ordered_queues)
