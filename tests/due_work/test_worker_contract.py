"""
RQ's contract with its worker, checked with due-work-harness.

due-work-harness (https://github.com/gigaverse-app/due-work-harness) is a pytest
plugin that checks background work is neither lost nor run twice. A contract
names the guarantees a system offers, out of six profiles (A to F), and binds
each claimed one to real code; the harness then generates the test cases.

This contract binds RQ itself, through ``worker_contract`` from the harness's RQ
integration and two real jobs defined in ``jobs.py``:

* profile B (ownership): a worker dequeues a job and prepares its execution the
  way RQ's worker does, ``maintain_heartbeats`` keeps the lease, and
  ``StartedJobRegistry.cleanup`` gives the job back once the lease expires;
* bounded retry: a job with ``Retry(max=2)`` whose service stays down runs
  three times through the worker, then stays failed;
* crash histories: ``SimpleWorker`` runs a job that sends a message and
  announces it through ``on_success``, and the harness kills it after each of
  its Redis commits, loses the reply to each commit, and fails each callback in
  turn. Recovery is RQ's own: later workers' maintenance, with the clock moved
  on so every lease has expired. Each history must end where normal operation
  does: the job finished, the message sent once, announced once. It runs once
  with a worker on the job's queue, and once with a worker on two queues, which
  RQ dequeues differently.

What diverges is declared as a gap, a strict xfail, and
``test_what_each_failure_costs`` pins what every history leaves.

Run it (Python 3.12 or later, and a Redis on localhost)::

    pip install -e . "due-work-harness>=0.3.0" time-machine   # or: uv sync --group dev
    pytest tests/due_work
"""

import pytest
from due_work_harness import Profile, due_work_contract_suite
from due_work_harness.crash_histories import ExternalCall, assert_pinned_outcomes
from due_work_harness.integrations.rq import worker_contract
from due_work_harness.integrations.task_queues import TaskOutcome

from rq import Callback, Queue, Retry

from . import jobs
from .connection import CONNECTION

QUEUE = 'due_work'
OTHER_QUEUE = 'due_work_other'
MESSAGE = 'your order has shipped'
MAX_RETRIES = 2


def a_message_owed() -> str:
    # ARRANGE: a job that sends one message and announces it, with one retry, ready for a worker.
    jobs.outbox.clear()
    job = Queue(QUEUE, connection=CONNECTION).enqueue(
        jobs.send_message,
        MESSAGE,
        retry=Retry(max=1),
        on_success=Callback(jobs.announce_success),
        on_failure=Callback(jobs.announce_failure),
    )
    return job.id


def what_happened(_job_id: str) -> tuple[int, tuple[str, ...]]:
    # OBSERVE: how many times the message left, and what the callbacks announced.
    return jobs.outbox.sent[MESSAGE], tuple(jobs.outbox.announced)


def a_failing_job() -> str:
    # ARRANGE: a job whose service stays down, with retries.
    jobs.outbox.clear()
    return Queue(QUEUE, connection=CONNECTION).enqueue(jobs.call_down_service, retry=Retry(max=MAX_RETRIES)).id


CONTRACT = worker_contract(
    CONNECTION,
    name='rq: the worker running a job',
    queue=QUEUE,
    other_queue=OTHER_QUEUE,
    enqueue=a_message_owed,
    effect=what_happened,
    enqueue_failing=a_failing_job,
    failures=lambda _job_id: jobs.outbox.failed_calls,
    max_retries=MAX_RETRIES,
    # EXTERNAL SEAM: the service the job sends its message to.
    external_calls=(ExternalCall(jobs.Outbox, 'send'),),
    gaps={
        Profile.B: {
            'assert_stale_token_is_rejected': (
                'handle_job_success and handle_job_failure settle a job without checking that the execution is '
                "still the job's owner: a worker whose lease expired, whose job StartedJobRegistry.cleanup gave "
                'back and another worker took, can still mark it finished, or send it back to be retried, while '
                'the new worker runs it'
            ),
        },
    },
    handoff_gaps={
        'the worker runs a task': (
            'a lost reply to the commit that records the job finished sends the finished job down the failure '
            'path: on_failure runs and the job is retried, so its message is sent twice; an on_success callback '
            'that raises does the same; and a worker that dies once the job has started makes RQ announce '
            'AbandonedJobError through on_failure, then run the job again. test_what_each_failure_costs pins '
            'each history'
        ),
        'a worker on two queues runs a task': (
            'a worker listening on more than one queue pops the job with LPOP, with no intermediate list: a death, '
            'or a lost reply, between the pop and the job being marked started leaves it queued in no queue and no '
            'registry, never run (https://github.com/rq/rq/issues/2236); the single-queue findings apply too. '
            'test_what_each_failure_costs pins each history'
        ),
    },
    fixtures=('empty_redis',),
)


# This is where the magic happens. The class is empty on purpose: the decorator reads CONTRACT
# and generates its tests, bound to RQ's real SimpleWorker, StartedJobRegistry.cleanup, heartbeats
# and Retry. No test case is written by hand; this file supplies only the jobs and how to see what
# they did, and the guarantees and their proofs come from the harness's RQ integration.
#
# Three of the generated cases are how the findings were made:
# - B-assert_stale_token_is_rejected lets a worker's lease expire, has cleanup give the job back
#   and a second worker take it, then has the first worker report its attempt. RQ accepts it.
# - The two handoff cases run the job through a worker once per thing that can go wrong: the
#   worker dies after each of its Redis writes, the reply to each write is lost, the on_success
#   callback raises. Each run is compared with a normal one. A lost reply after the job is
#   recorded finished runs it again; a worker on two queues can lose the job outright.
# Each is declared as a gap, so it is reported as a strict XFAIL; the day RQ holds the guarantee,
# the case passes, and the strict marker fails the run until the gap is removed.
@due_work_contract_suite(CONTRACT)
class TestWorkerContract:
    """Every case in this class is generated from CONTRACT; see the comment above."""


def _job(status: str, sent: int, *announced: str) -> TaskOutcome:
    return TaskOutcome(status=status, effect=(sent, announced))


SENT = f'sent {MESSAGE!r}'
DELIVERED = _job('finished', 1, SENT)
#: The job left its queue and nothing will ever run it.
LOST = _job('queued', 0)

# What each history leaves after RQ's recovery, pinned; every history not listed reaches DELIVERED.
# Commit numbers count the worker's Redis writes: its registration and heartbeats come first. A
# change in RQ's worker moves an entry, and the test names the one that moved.
SINGLE_QUEUE_FINDINGS = {
    # The job started, the worker died before running it: RQ reports AbandonedJobError to on_failure,
    # then the retry runs it. The customer was told it failed.
    'worker died after commit 13': _job('finished', 1, 'failed: AbandonedJobError', SENT),
    # At least once: the message was sent, the worker died before recording it, the retry sends it again.
    'worker died after commit 14': _job('finished', 2, 'failed: AbandonedJobError', SENT),
    'worker died after external call 1': _job('finished', 2, 'failed: AbandonedJobError', SENT),
    # The job was marked started, but the worker never saw the reply: it fails the attempt and retries.
    'the reply to commit 13 was lost': _job('finished', 1, 'failed: ConnectionError', SENT),
    'the reply to commit 14 was lost': _job('finished', 2, 'failed: ConnectionError', SENT),
    # FINDING: the job was recorded finished and announced; the lost reply sends it down the failure
    # path, which announces a failure and runs it again.
    'the reply to commit 15 was lost': _job('finished', 2, SENT, 'failed: ConnectionError', SENT),
    # FINDING: on_success raised after the message was sent; RQ fails the job and the retry sends it again.
    'signal receiver 1 failed': _job('finished', 2, 'failed: ReceiverFailed', SENT),
}
TWO_QUEUE_FINDINGS = {
    # FINDING (#2236): popped with LPOP, then lost: nothing in any queue or registry points at the job.
    **{f'worker died after commit {k}': LOST for k in range(9, 14)},
    **{f'the reply to commit {k} was lost': LOST for k in range(9, 14)},
    'worker died after commit 14': _job('finished', 1, 'failed: AbandonedJobError', SENT),
    'worker died after commit 15': _job('finished', 2, 'failed: AbandonedJobError', SENT),
    'worker died after external call 1': _job('finished', 2, 'failed: AbandonedJobError', SENT),
    'the reply to commit 14 was lost': _job('finished', 1, 'failed: ConnectionError', SENT),
    'the reply to commit 15 was lost': _job('finished', 2, 'failed: ConnectionError', SENT),
    'the reply to commit 16 was lost': _job('finished', 2, SENT, 'failed: ConnectionError', SENT),
    'signal receiver 1 failed': _job('finished', 2, 'failed: ReceiverFailed', SENT),
}


@pytest.mark.parametrize(
    ('history', 'findings'),
    [(CONTRACT.handoffs[0], SINGLE_QUEUE_FINDINGS), (CONTRACT.handoffs[1], TWO_QUEUE_FINDINGS)],
    ids=['one queue', 'two queues'],
)
def test_what_each_failure_costs(empty_redis, history, findings) -> None:
    assert_pinned_outcomes(CONTRACT.handoff_delivery, history, delivered=DELIVERED, outcomes=findings)
