# Fix Race Condition: Canceled Jobs Executed by Worker (Fixes #2160)

## Description
This PR fixes a race condition where a job that is canceled after being dequeued but before execution starts could still be executed by the worker. This addresses Issue #2160.

The issue was that `prepare_job_execution` would unconditionally set the job status to `STARTED`, overwriting any `CANCELED` status set by `job.cancel()` in the interim.

## The Issue (from #2160)
As reported, canceled jobs can sometimes be started.
```python
# checking logs
1733422418.602015, f7b21b30..., canceled: canceled
1733422418.640335, 23afc67f..., started: Starting work! (Should NOT happen)
```
This occurs because there is a window between the worker fetching the job and the worker setting the job status to `STARTED` where the job can be canceled by another client.

## Changes
- Modified `rq/worker/base.py`:
  - **`prepare_job_execution`**: Wrapped the job status update in a Redis `WATCH` transaction. It now checks if the job status is `CANCELED` before proceeding. If canceled, it raises `InvalidJobOperation`.
  - **`perform_job`**: Added logic to catch `InvalidJobOperation`. If the job is canceled, it logs a warning and skips execution (returns `False`).

## How to Test
1. **Unit Tests**:
   Run the newly added test suite which simulates the race condition using mocks:
   ```bash
   pytest tests/test_cancellation_race_condition.py
   ```
   Ensure all tests pass.

2. **Reproduction Script** (Optional):
   If you have a reproduction script (like `check_rq.py` provided in the issue), run it against this branch. Canceled jobs should no longer enter the `STARTED` state.

## Verification
- Added `tests/test_cancellation_race_condition.py`.
- Verified that `prepare_job_execution` aborts if job is canceled.
- Verified that `perform_job` gracefully handles the abortion.
