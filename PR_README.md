# Fix Race Condition in Job Cancellation

## Description
This PR fixes a race condition where canceled jobs could still be started by a worker.

## The Issue
Previously, there was a window between a worker fetching a job and starting its execution where the job could be canceled, but the worker would proceed to execute it anyway. This happened because the worker did not re-verify the job's status immediately before execution in a transaction safe manner.

## The Fix
The fix involves updating `Worker.prepare_job_execution` to:
1. Watch the job key in Redis.
2. Check if the job status is `CANCELED`.
3. If canceled, abort execution and raise `InvalidJobOperation`.
4. If not canceled, proceed with execution preparation within a Redis transaction (pipeline).

This ensures that if a job is canceled after being dequeued but before execution preparation is committed, the `WATCH` mechanism will trigger a `WatchError`, causing the worker to retry the preparation loop, re-check the status, and correctly identify the cancellation.

## Verification
The fix has been verified with a reproduction script (`check_rq.py`) that intentionally creates a race condition by canceling jobs immediately after enqueueing them.

### Reproduction Steps
1. Start Redis.
2. Run `check_rq.py` which enqueues jobs and immediately cancels them.
3. Observe that without the fix, some canceled jobs enter the `STARTED` state.
4. With the fix, all canceled jobs are correctly identified and do not execute.

## Tests
- Added verification using `check_rq.py`.
- Ran existing tests in `tests/test_worker.py`.

## Breaking Changes
None.
