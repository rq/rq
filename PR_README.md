# Fix Race Condition: Canceled Jobs Executed by Worker

## Description
This PR addresses Issue #2160 where canceled jobs can sometimes be started and executed by the worker. This occurs when a job is canceled after being dequeued but before execution begins.

### The Problem
As described in the issue, jobs that are canceled should not be executed. However, under race conditions, a worker might pick up a job, and before it starts executing, the job is canceled. Currently, `prepare_job_execution` unconditionally sets the job status to `STARTED`, overwriting the `CANCELED` status and causing the job to run.

#### Reproduction Logs
```
1733422418.602015, f7b21b30..., canceled: canceled
...
1733422418.640335, 23afc67f..., started: Starting work! (Should NOT happen)
```

## Changes
I have modified `rq/worker/base.py` to ensure atomic status updates.

- **`rq/worker/base.py`**:
  - **`prepare_job_execution`**: Now uses a Redis `WATCH` transaction. It checks if the job status is `CANCELED` before setting it to `STARTED`. If the job is canceled, it raises an `InvalidJobOperation` exception.
  - **`perform_job`**: Added a `try/except` block to catch `InvalidJobOperation`. If caught and the job is canceled, it logs a warning and skips execution (`return False`).

## Verification
I have added a new test case `TestCancellationRaceCondition` to `tests/test_worker.py`. Since a local Redis server was unavailable for integration testing, these tests use `unittest.mock` to verify the logic in isolation.

### Tests
- `test_prepare_job_execution_aborts_if_canceled`: Verifies that `prepare_job_execution` aborts and raises exception if job is canceled.
- `test_prepare_job_execution_proceeds_if_not_canceled`: Verifies that normal execution flow is preserved for queued jobs.
- `test_perform_job_handles_cancellation_exception`: Verifies that `perform_job` catches the exception and returns `False` (skipping execution) when a job is canceled.
