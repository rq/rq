from __future__ import annotations

from datetime import datetime
from functools import cached_property

from redis import Redis
from redis.client import Pipeline

from .utils import as_text, current_timestamp, now, utcformat


class RateLimit:
    """Defines a concurrency-based rate limit for jobs.

    Args:
        key: A string key that groups jobs together for rate limiting.
        concurrency: Maximum number of jobs with this key that can be
            queued or executing at the same time.
    """

    def __init__(self, key: str, concurrency: int):
        if not key:
            raise ValueError('key must not be empty')
        if concurrency < 1:
            raise ValueError('concurrency must be at least 1')
        self.key = key
        self.concurrency = concurrency


# Lua script: Atomically acquire capacity and enqueue the next pending job.
# Checks if active count < max_concurrency. If capacity available, pops pending
# jobs until it finds a valid one (origin set and status == 'rate_limited'),
# discarding stale entries along the way, then ZADD to active, RPUSH to queue,
# HSET status to queued.
# Returns enqueued job_id or nil.
# KEYS: active_key, pending_key
# ARGV: max_concurrency, timestamp, enqueued_at
ACQUIRE_AND_ENQUEUE_SCRIPT = """
local active_count = redis.call('ZCARD', KEYS[1])
local max_concurrency = tonumber(ARGV[1])
local timestamp = tonumber(ARGV[2])
local enqueued_at = ARGV[3]

if active_count < max_concurrency then
    while true do
        local result = redis.call('ZPOPMIN', KEYS[2])
        if #result == 0 then
            return nil
        end
        local job_id = result[1]
        local origin = redis.call('HGET', 'rq:job:' .. job_id, 'origin')
        local status = redis.call('HGET', 'rq:job:' .. job_id, 'status')
        if origin and status == 'rate_limited' then
            redis.call('ZADD', KEYS[1], timestamp, job_id)
            redis.call('RPUSH', 'rq:queue:' .. origin, job_id)
            redis.call('HSET', 'rq:job:' .. job_id, 'status', 'queued', 'enqueued_at', enqueued_at)
            return job_id
        end
        -- stale pending job (missing hash, no origin, or non-rate_limited status):
        -- it's already popped, so loop to the next
    end
end
return nil
"""

# Lua script: Release capacity from a completed job and enqueue the next pending job.
# Removes completed job from active, then runs the same acquire logic.
# Returns enqueued job_id or nil.
# KEYS: active_key, pending_key
# ARGV: completed_job_id, max_concurrency, timestamp, enqueued_at
RELEASE_CAPACITY_SCRIPT = """
local completed_job_id = ARGV[1]
local max_concurrency = tonumber(ARGV[2])
local timestamp = tonumber(ARGV[3])
local enqueued_at = ARGV[4]

redis.call('ZREM', KEYS[1], completed_job_id)

local active_count = redis.call('ZCARD', KEYS[1])
if active_count < max_concurrency then
    while true do
        local result = redis.call('ZPOPMIN', KEYS[2])
        if #result == 0 then
            return nil
        end
        local job_id = result[1]
        local origin = redis.call('HGET', 'rq:job:' .. job_id, 'origin')
        local status = redis.call('HGET', 'rq:job:' .. job_id, 'status')
        if origin and status == 'rate_limited' then
            redis.call('ZADD', KEYS[1], timestamp, job_id)
            redis.call('RPUSH', 'rq:queue:' .. origin, job_id)
            redis.call('HSET', 'rq:job:' .. job_id, 'status', 'queued', 'enqueued_at', enqueued_at)
            return job_id
        end
        -- stale pending job (missing hash, no origin, or non-rate_limited status):
        -- it's already popped, so loop to the next
    end
end
return nil
"""


# Lua script: Atomically clean up an empty rate limit registry.
# If both active and pending sets are empty, removes the key from
# rq:rl-keys, and deletes the config hash and sorted sets.
# KEYS: active_key, pending_key, config_key
# ARGV: rl_keys_key, key
# Returns 1 if cleaned up, 0 if not empty.
CLEANUP_REGISTRY_SCRIPT = """
if redis.call('ZCARD', KEYS[1]) == 0 and redis.call('ZCARD', KEYS[2]) == 0 then
    redis.call('SREM', ARGV[1], ARGV[2])
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
    return 1
end
return 0
"""


class RateLimitRegistry:
    """Manages the active and pending sorted sets for a rate limit key.

    Each rate limit key has:
    - rq:rl:{key} — a hash storing config (e.g., concurrency)
    - rq:rl:{key}:active — sorted set of job IDs holding capacity
    - rq:rl:{key}:pending — sorted set of job IDs waiting for capacity
    """

    rl_keys_key = 'rq:rl-keys'

    def __init__(self, key: str, connection: Redis):
        self.key = key
        self.connection = connection
        self._acquire_script = connection.register_script(ACQUIRE_AND_ENQUEUE_SCRIPT)
        self._release_script = connection.register_script(RELEASE_CAPACITY_SCRIPT)
        self._cleanup_script = connection.register_script(CLEANUP_REGISTRY_SCRIPT)

    def register(self, max_concurrency: int, pipeline: Pipeline) -> None:
        """Register this rate limit key and persist its config."""
        pipeline.sadd(self.rl_keys_key, self.key)
        pipeline.hset(self.config_key, 'concurrency', max_concurrency)

    @cached_property
    def max_concurrency(self) -> int:
        """Read max_concurrency from the config hash in Redis."""
        value = self.connection.hget(self.config_key, 'concurrency')
        return int(value) if value else 0

    @classmethod
    def all(cls, connection: Redis) -> list[RateLimitRegistry]:
        """Returns all known RateLimitRegistry instances."""
        keys = connection.smembers(cls.rl_keys_key)
        return [cls(key=as_text(key), connection=connection) for key in keys]

    @property
    def config_key(self) -> str:
        return f'rq:rl:{self.key}'

    @property
    def active_key(self) -> str:
        return f'rq:rl:{self.key}:active'

    @property
    def pending_key(self) -> str:
        return f'rq:rl:{self.key}:pending'

    def get_active_job_ids(self) -> list[str]:
        """Returns job IDs in the active set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.active_key, 0, -1)]

    def get_pending_job_ids(self) -> list[str]:
        """Returns job IDs in the pending set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.pending_key, 0, -1)]

    def get_active_job_count(self) -> int:
        """Returns the number of jobs in the active set."""
        return self.connection.zcard(self.active_key)

    def get_pending_job_count(self) -> int:
        """Returns the number of jobs in the pending set."""
        return self.connection.zcard(self.pending_key)

    def add_to_pending(self, job_id: str, pipeline: Pipeline, timestamp: float | None = None) -> None:
        """Add a job to the pending set."""
        if timestamp is None:
            timestamp = current_timestamp()
        pipeline.zadd(self.pending_key, {job_id: timestamp})

    def acquire_and_enqueue(self, max_concurrency: int, enqueued_at: datetime | None = None) -> str | None:
        """Try to enqueue the next pending job.

        Atomically checks if there's capacity, and if so pops from pending,
        adds to active, reads the job's origin to determine the queue,
        pushes to the queue, and sets the job status to queued.

        Args:
            max_concurrency: Maximum number of concurrent jobs allowed.
            enqueued_at: The timestamp to record as the job's `enqueued_at`.
                Defaults to the current time. Callers can pass this so they can
                mirror the stored value onto the in-memory job without a re-read.

        Returns:
            The enqueued job_id, or None if no capacity or no pending jobs.
        """
        if enqueued_at is None:
            enqueued_at = now()
        timestamp = current_timestamp()
        result = self._acquire_script(
            keys=[self.active_key, self.pending_key],
            args=[max_concurrency, timestamp, utcformat(enqueued_at)],
        )
        if result is not None:
            return as_text(result)
        return None

    def release_capacity_and_enqueue(self, job_id: str) -> str | None:
        """Release capacity from a completed job and enqueue the next pending job.

        Atomically removes the job from active, then tries to enqueue the next
        pending job (same logic as acquire_and_enqueue).

        Args:
            job_id: The completed job's ID to remove from active.

        Returns:
            The enqueued job_id, or None if no pending jobs.
        """
        timestamp = current_timestamp()
        result = self._release_script(
            keys=[self.active_key, self.pending_key],
            args=[job_id, self.max_concurrency, timestamp, utcformat(now())],
        )
        if result is not None:
            return as_text(result)
        return None

    def cancel(self, job_id: str, pipeline: Pipeline | None = None) -> str | None:
        """Remove a job from rate limit tracking and enqueue the next pending job if needed.

        Args:
            job_id: The job ID to remove.
            pipeline: If provided, only the ZREM (active + pending) ops are buffered onto
                the caller's transaction and no job is promoted — promotion is left to the
                next release/acquire or maintenance cleanup, since the caller may still
                discard the transaction. If None, removal runs immediately and, if the job
                was active, the next pending job is promoted.

        Returns:
            The enqueued job_id, or None.
        """
        if pipeline is not None:
            pipeline.zrem(self.active_key, job_id)
            pipeline.zrem(self.pending_key, job_id)
            return None

        was_active = self.connection.zrem(self.active_key, job_id)
        self.connection.zrem(self.pending_key, job_id)
        if was_active:
            return self.acquire_and_enqueue(self.max_concurrency)
        return None

    def _release_stale_active_jobs(self) -> None:
        """Free active slots whose job no longer exists or is not in a state
        that legitimately holds a slot (queued or started).

        Any other state — missing, terminal, scheduled or malformed — means the
        slot leaked and should be freed so pending jobs can proceed.
        """
        from .job import Job, JobStatus  # local import avoids circular import

        active_statuses = (JobStatus.QUEUED, JobStatus.STARTED)
        job_ids = self.get_active_job_ids()
        if not job_ids:
            return

        # Read only the status field, defensively. We avoid hydrating full Job
        # objects (Job.restore raises on a malformed status), so an unknown or
        # missing status is simply not in the allowlist and treated as stale.
        with self.connection.pipeline() as pipeline:
            for job_id in job_ids:
                pipeline.hget(Job.key_for(job_id), 'status')
            raw_statuses = pipeline.execute()

        for job_id, raw_status in zip(job_ids, raw_statuses):
            status = as_text(raw_status) if raw_status else None
            if status not in active_statuses:
                self.release_capacity_and_enqueue(job_id)

    def cleanup(self) -> None:
        """Free stale active slots, enqueue pending jobs if there is available
        capacity, then remove the registry if both active and pending are empty.

        Called during periodic maintenance to handle cases where jobs are stuck
        in pending (e.g., worker crashed before releasing capacity) or where a
        job left the active set holding a slot it should have released.
        """
        self._release_stale_active_jobs()

        if self.max_concurrency and self.acquire_and_enqueue(self.max_concurrency):
            return

        # Atomically remove registry if empty
        self._cleanup_script(
            keys=[self.active_key, self.pending_key, self.config_key],
            args=[self.rl_keys_key, self.key],
        )
