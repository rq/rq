# Fix Race Condition: Canceled Jobs Executed by Worker

## Description
This PR fixes a race condition where a job that is canceled after being dequeued but before execution starts could still be executed by the worker.

The issue was that `prepare_job_execution` would unconditionally set the job status to `STARTED`, overwriting any `CANCELED` status set by `job.cancel()` in the interim.

## Changes
- Modified `rq/worker/base.py`:
  - `prepare_job_execution`: Wrapped the job status update in a Redis `WATCH` transaction. It now checks if the job status is `CANCELED` before proceeding. If canceled, it raises `InvalidJobOperation`.
  - `perform_job`: Added logic to catch `InvalidJobOperation`. If the job is canceled, it logs a warning and skips execution (returns `False`).

## Verification
- Added a new unit test file `tests/test_cancellation_race_condition.py` which mocks Redis connections to verify the fix logic in isolation (since local Redis was unavailable).
- Verified that:
  - `prepare_job_execution` aborts if job is canceled.
  - `perform_job` gracefully handles the abortion and skips execution.
  - Normal execution proceeds if job is not canceled.
