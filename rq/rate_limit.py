from redis import Redis

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
# Checks if active count < max_concurrency. If capacity available:
#   ZPOPMIN from pending, ZADD to active, RPUSH to queue, HSET status to queued.
# Returns enqueued job_id or nil.
ACQUIRE_AND_ENQUEUE_SCRIPT = """
local active_count = redis.call('ZCARD', KEYS[1])
local max_concurrency = tonumber(ARGV[1])
local timestamp = tonumber(ARGV[2])
local enqueued_at = ARGV[3]

if active_count < max_concurrency then
    local result = redis.call('ZPOPMIN', KEYS[2])
    if #result > 0 then
        local job_id = result[1]
        redis.call('ZADD', KEYS[1], timestamp, job_id)
        redis.call('RPUSH', KEYS[3], job_id)
        redis.call('HSET', 'rq:job:' .. job_id, 'status', 'queued', 'enqueued_at', enqueued_at)
        return job_id
    end
end
return nil
"""

# Lua script: Release capacity from a completed job and enqueue the next pending job.
# Removes completed job from active, then runs the same acquire logic.
# Returns enqueued job_id or nil.
RELEASE_CAPACITY_SCRIPT = """
local completed_job_id = ARGV[1]
local max_concurrency = tonumber(ARGV[2])
local timestamp = tonumber(ARGV[3])
local enqueued_at = ARGV[4]

redis.call('ZREM', KEYS[1], completed_job_id)

local active_count = redis.call('ZCARD', KEYS[1])
if active_count < max_concurrency then
    local result = redis.call('ZPOPMIN', KEYS[2])
    if #result > 0 then
        local job_id = result[1]
        redis.call('ZADD', KEYS[1], timestamp, job_id)
        redis.call('RPUSH', KEYS[3], job_id)
        redis.call('HSET', 'rq:job:' .. job_id, 'status', 'queued', 'enqueued_at', enqueued_at)
        return job_id
    end
end
return nil
"""


class RateLimitRegistry:
    """Manages the active and pending sorted sets for a rate limit key.

    Each rate limit key has two Redis sorted sets:
    - active: jobs currently holding capacity (queued or executing)
    - pending: jobs waiting for capacity (FIFO by timestamp)
    """

    redis_queue_namespace_prefix = 'rq:queue:'

    def __init__(self, key: str, connection: Redis):
        self.key = key
        self.connection = connection
        self._acquire_script = connection.register_script(ACQUIRE_AND_ENQUEUE_SCRIPT)
        self._release_script = connection.register_script(RELEASE_CAPACITY_SCRIPT)

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

    def add_to_pending(self, job_id: str, timestamp: float | None = None) -> None:
        """Add a job to the pending set."""
        if timestamp is None:
            timestamp = current_timestamp()
        self.connection.zadd(self.pending_key, {job_id: timestamp})

    def _get_queue_key(self, queue_name: str) -> str:
        """Derive the Redis queue key from a queue name."""
        return f'{self.redis_queue_namespace_prefix}{queue_name}'

    def acquire_and_enqueue(self, queue_name: str, max_concurrency: int) -> str | None:
        """Try to enqueue the next pending job.

        Atomically checks if there's capacity, and if so pops from pending,
        adds to active, pushes to the queue, and sets the job status to queued.

        Args:
            queue_name: The name of the queue to push the job into.
            max_concurrency: Maximum number of concurrent jobs allowed.

        Returns:
            The enqueued job_id, or None if no capacity or no pending jobs.
        """
        queue_key = self._get_queue_key(queue_name)
        timestamp = current_timestamp()
        result = self._acquire_script(
            keys=[self.active_key, self.pending_key, queue_key],
            args=[max_concurrency, timestamp, utcformat(now())],
        )
        if result is not None:
            return as_text(result)
        return None

    def release_capacity_and_enqueue(self, queue_name: str, job_id: str, max_concurrency: int) -> str | None:
        """Release capacity from a completed job and enqueue the next pending job.

        Atomically removes the job from active, then tries to enqueue the next
        pending job (same logic as acquire_and_enqueue).

        Args:
            queue_name: The name of the queue to push the enqueued job into.
            job_id: The completed job's ID to remove from active.
            max_concurrency: Maximum number of concurrent jobs allowed.

        Returns:
            The enqueued job_id, or None if no pending jobs.
        """
        queue_key = self._get_queue_key(queue_name)
        timestamp = current_timestamp()
        result = self._release_script(
            keys=[self.active_key, self.pending_key, queue_key],
            args=[job_id, max_concurrency, timestamp, utcformat(now())],
        )
        if result is not None:
            return as_text(result)
        return None

    def cancel(self, queue_name: str, job_id: str, max_concurrency: int) -> str | None:
        """Remove a job from rate limit tracking and enqueue the next pending job if needed.

        Args:
            queue_name: The name of the queue for enqueueing the next job.
            job_id: The job ID to remove.
            max_concurrency: Maximum number of concurrent jobs allowed.

        Returns:
            The enqueued job_id, or None.
        """
        was_active = self.connection.zrem(self.active_key, job_id)
        self.connection.zrem(self.pending_key, job_id)
        if was_active:
            return self.acquire_and_enqueue(queue_name, max_concurrency)
        return None
