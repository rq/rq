---
title: "RQ: Rate Limiting Internals"
layout: docs
---

This page explains how RQ's rate limiting works under the hood and why it is built the
way it is. It's aimed at contributors; see the [user guide](/docs/#concurrency-rate-limits)
for the public API and supported behavior.

The strategy implemented today is **concurrency-based**: it caps how many jobs sharing
a key can be queued or executing at the same time, across all workers and queues using
the same Redis database.

## Terminology

- **slot** — one unit of capacity; a key with `concurrency=N` has N slots.
- **allowed** — jobs currently holding a slot (queued or executing).
- **rate_limited** — jobs waiting for a slot; also the `JobStatus` they carry.
- **promote** — move the oldest waiting job into `allowed` and push it onto its queue.
- **rate limit registry** — the per-key bookkeeper (`RateLimitRegistry`) that maintains
  the `allowed` and `rate_limited` sets and performs promotion.

## The Mental Model

A step-by-step example — three jobs sharing a rate limit that allows two at a time:

```python
rate_limit = RateLimit(key='reports', concurrency=2)

job_a = queue.enqueue(generate_report, rate_limit=rate_limit)  # slot free → queued
job_b = queue.enqueue(generate_report, rate_limit=rate_limit)  # slot free → queued
job_c = queue.enqueue(generate_report, rate_limit=rate_limit)  # no slots left → rate_limited
```

Contents of the queue and the rate limit registry (its `allowed` and `rate_limited`
sets) after each event:

| Event            | Queue        | Allowed      | Rate Limited |
|------------------|--------------|--------------|--------------|
| enqueue `job_a`  | job_a        | job_a        |              |
| enqueue `job_b`  | job_a, job_b | job_a, job_b |              |
| enqueue `job_c`  | job_a, job_b | job_a, job_b | job_c        |
| `job_a` finishes | job_b, job_c | job_b, job_c |              |

The key subtlety: a `rate_limited` job exists only as a job hash plus an entry in the
`rate_limited` sorted set. It is **not on any queue** — workers cannot see it until
promotion pushes it onto its origin queue.

Enqueueing always goes through the rate limit registry: every rate-limited job is
saved as `rate_limited` and added to the waiting set first, then promotion runs — when
capacity is free, the job promoted is usually the one just parked. This single
admission path keeps waiting jobs FIFO (nothing jumps ahead of existing waiters), puts the
capacity decision in exactly one place and makes plain `enqueue()`, scheduled jobs and
resolved dependents behave identically.

## Data Model in Redis

Everything lives in `rq/rate_limit.py` (`RateLimit`, `RateLimitRegistry`). Each rate
limit key has one registry, stored as:

- `rq:rl:{key}` — config hash, stores `concurrency`.
- `rq:rl:{key}:allowed` — sorted set of jobs holding a slot, scored by acquire time.
- `rq:rl:{key}:rate_limited` — sorted set of waiting jobs, scored by enqueue time, so
  `ZPOPMIN` promotes oldest-first.
- `rq:rl-keys` — set of all known rate limit keys, so maintenance can sweep every
  registry without scanning the keyspace.

Jobs persist `rate_limit_key` and `rate_limit_concurrency` on their hash;
`Job.has_rate_limit` is true when both are set.

## The Two Operations and Why They're Lua

- `acquire_and_enqueue` — if `ZCARD(allowed) < concurrency`, pop the oldest waiting
  job, add it to `allowed`, push it onto its origin queue and mark it `queued`.
- `release_and_enqueue` — remove a job from `allowed`, then run the same
  acquire logic. The release script is the acquire script with one `ZREM` prepended —
  a single shared body, so the two can't drift.

Each operation runs as a single Lua script, so the capacity check and the promotion
execute atomically in Redis and cannot interleave across concurrent workers.

## Interactions Worth Knowing

- **Retries** — an immediate retry (interval 0) keeps its slot and reruns on it. A
  delayed retry releases the slot — it may sit scheduled for hours, and holding a slot
  that long would starve the key — then re-acquires when due.
- **Cancel and delete** — both remove the job from the rate limit sets and promote the
  next waiting job.

## Rate Limit Registry Cleanup

Rate limit state can go stale: a worker can crash after taking a slot and before
releasing it, and deferred promotions leave freed capacity while jobs are still
waiting. `RateLimitRegistry.cleanup()` reconciles this. It runs as part of
`clean_registries`, the periodic registry maintenance performed by workers, and:

1. Releases stale `allowed` entries and attempts to promote a waiter after each release.
2. Attempts another promotion in case capacity was freed elsewhere.
3. Deletes the registry once both sets are empty.

## Known Sharp Edges

- **Per-key concurrency drift** — `concurrency` is stored once per key and the last
  registrant wins, but enqueue-time acquire uses the per-job value while release reads
  the stored config. Two jobs registering different values for the same key can
  over-admit.
- **Delayed retry placement** — `Retry(enqueue_at_front=True)` is not honored when a
  delayed retry re-enters through the rate limiter; promotion uses the job's original
  enqueue placement.
- **Returned job status can briefly lag** — `enqueue()` returns an in-memory job whose
  status can be stale if a concurrent release promoted it in a narrow window;
  `refresh()` corrects it.
