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


# Lua: if allowed < max_concurrency, pop the next valid rate_limited job (skipping
# stale entries), add it to allowed, RPUSH to its queue and mark it queued.
# Returns the enqueued job_id or nil.
# KEYS: allowed_key, rate_limited_key
# ARGV: max_concurrency, timestamp, enqueued_at
ACQUIRE_AND_ENQUEUE_SCRIPT = """
local allowed_count = redis.call('ZCARD', KEYS[1])
local max_concurrency = tonumber(ARGV[1])
local timestamp = tonumber(ARGV[2])
local enqueued_at = ARGV[3]

if allowed_count < max_concurrency then
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
        -- stale rate_limited job (missing hash, no origin, or non-rate_limited status):
        -- it's already popped, so loop to the next
    end
end
return nil
"""

# Release = remove the completed job from allowed (ARGV[4]) then run the acquire script.
# ARGV: max_concurrency, timestamp, enqueued_at, completed_job_id
RELEASE_AND_ENQUEUE_SCRIPT = "redis.call('ZREM', KEYS[1], ARGV[4])\n" + ACQUIRE_AND_ENQUEUE_SCRIPT


# Lua: if both allowed and rate_limited sets are empty, drop the key from rq:rl-keys
# and delete the config hash and sorted sets. Returns 1 if cleaned up, 0 if not empty.
# KEYS: allowed_key, rate_limited_key, config_key
# ARGV: rl_keys_key, key
CLEANUP_REGISTRY_SCRIPT = """
if redis.call('ZCARD', KEYS[1]) == 0 and redis.call('ZCARD', KEYS[2]) == 0 then
    redis.call('SREM', ARGV[1], ARGV[2])
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
    return 1
end
return 0
"""


class RateLimitRegistry:
    """Manages the allowed and rate_limited sorted sets for a rate limit key.

    Each rate limit key has:
    - rq:rl:{key} — a hash storing config (e.g., concurrency)
    - rq:rl:{key}:allowed — sorted set of job IDs the limiter has let through
    - rq:rl:{key}:rate_limited — sorted set of job IDs the limiter is holding back
    """

    rl_keys_key = 'rq:rl-keys'

    def __init__(self, key: str, connection: Redis):
        self.key = key
        self.connection = connection
        self._acquire_script = connection.register_script(ACQUIRE_AND_ENQUEUE_SCRIPT)
        self._release_script = connection.register_script(RELEASE_AND_ENQUEUE_SCRIPT)
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
    def allowed_key(self) -> str:
        return f'rq:rl:{self.key}:allowed'

    @property
    def rate_limited_key(self) -> str:
        return f'rq:rl:{self.key}:rate_limited'

    def get_allowed_job_ids(self) -> list[str]:
        """Returns job IDs in the allowed set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.allowed_key, 0, -1)]

    def get_rate_limited_job_ids(self) -> list[str]:
        """Returns job IDs in the rate_limited set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.rate_limited_key, 0, -1)]

    def get_allowed_job_count(self) -> int:
        """Returns the number of jobs in the allowed set."""
        return self.connection.zcard(self.allowed_key)

    def get_rate_limited_job_count(self) -> int:
        """Returns the number of jobs in the rate_limited set."""
        return self.connection.zcard(self.rate_limited_key)

    def add_to_rate_limited(self, job_id: str, pipeline: Pipeline, timestamp: float | None = None) -> None:
        """Add a job to the rate_limited set."""
        if timestamp is None:
            timestamp = current_timestamp()
        pipeline.zadd(self.rate_limited_key, {job_id: timestamp})

    def acquire_and_enqueue(self, max_concurrency: int, enqueued_at: datetime | None = None) -> str | None:
        """Try to enqueue the next rate_limited job.

        Atomically checks if there's capacity, and if so pops from rate_limited,
        adds to allowed, reads the job's origin to determine the queue,
        pushes to the queue, and sets the job status to queued.

        Args:
            max_concurrency: Maximum number of concurrent jobs allowed.
            enqueued_at: The timestamp to record as the job's `enqueued_at`.
                Defaults to the current time. Callers can pass this so they can
                mirror the stored value onto the in-memory job without a re-read.

        Returns:
            The enqueued job_id, or None if no capacity or no rate_limited jobs.
        """
        if enqueued_at is None:
            enqueued_at = now()
        timestamp = current_timestamp()
        result = self._acquire_script(
            keys=[self.allowed_key, self.rate_limited_key],
            args=[max_concurrency, timestamp, utcformat(enqueued_at)],
        )
        if result is not None:
            return as_text(result)
        return None

    def release_and_enqueue(self, job_id: str) -> str | None:
        """Release capacity from a completed job and enqueue the next rate_limited job.

        Atomically removes the job from allowed, then tries to enqueue the next
        rate_limited job (same logic as acquire_and_enqueue).

        Args:
            job_id: The completed job's ID to remove from allowed.

        Returns:
            The enqueued job_id, or None if no rate_limited jobs.
        """
        timestamp = current_timestamp()
        result = self._release_script(
            keys=[self.allowed_key, self.rate_limited_key],
            args=[self.max_concurrency, timestamp, utcformat(now()), job_id],
        )
        if result is not None:
            return as_text(result)
        return None

    def cancel(self, job_id: str, pipeline: Pipeline | None = None) -> str | None:
        """Remove a job from rate limit tracking and enqueue the next rate_limited job if needed.

        Args:
            job_id: The job ID to remove.
            pipeline: If provided, only the ZREM (allowed + rate_limited) ops are buffered onto
                the caller's transaction and no job is promoted — promotion is left to the
                next release/acquire or maintenance cleanup, since the caller may still
                discard the transaction. If None, removal runs immediately and, if the job
                was allowed, the next rate_limited job is promoted.

        Returns:
            The enqueued job_id, or None.
        """
        if pipeline is not None:
            pipeline.zrem(self.allowed_key, job_id)
            pipeline.zrem(self.rate_limited_key, job_id)
            return None

        was_allowed = self.connection.zrem(self.allowed_key, job_id)
        self.connection.zrem(self.rate_limited_key, job_id)
        if was_allowed:
            return self.acquire_and_enqueue(self.max_concurrency)
        return None

    def _release_stale_allowed_jobs(self) -> None:
        """Free allowed slots whose job no longer exists or is not in a state
        that legitimately holds a slot (queued or started).

        Any other state — missing, terminal, scheduled or malformed — means the
        slot leaked and should be freed so rate_limited jobs can proceed.
        """
        from .job import Job, JobStatus  # local import avoids circular import

        allowed_statuses = (JobStatus.QUEUED, JobStatus.STARTED)
        job_ids = self.get_allowed_job_ids()
        if not job_ids:
            return

        # Read only the status field — hydrating a full Job (Job.restore) raises on a
        # malformed status; here an unknown/missing status is just treated as stale.
        with self.connection.pipeline() as pipeline:
            for job_id in job_ids:
                pipeline.hget(Job.key_for(job_id), 'status')
            raw_statuses = pipeline.execute()

        for job_id, raw_status in zip(job_ids, raw_statuses):
            status = as_text(raw_status) if raw_status else None
            if status not in allowed_statuses:
                self.release_and_enqueue(job_id)

    def cleanup(self) -> None:
        """Free stale allowed slots, enqueue rate_limited jobs if there is available
        capacity, then remove the registry if both allowed and rate_limited are empty.

        Called during periodic maintenance to handle cases where jobs are stuck
        in rate_limited (e.g., worker crashed before releasing capacity) or where a
        job left the allowed set holding a slot it should have released.
        """
        self._release_stale_allowed_jobs()

        if self.max_concurrency and self.acquire_and_enqueue(self.max_concurrency):
            return

        # Atomically remove registry if empty
        self._cleanup_script(
            keys=[self.allowed_key, self.rate_limited_key, self.config_key],
            args=[self.rl_keys_key, self.key],
        )
