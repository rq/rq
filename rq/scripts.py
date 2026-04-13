import calendar
import logging
import time
from datetime import timedelta, timezone

from .exceptions import DuplicateJobError
from .logutils import blue, green

logger = logging.getLogger('rq.scripts')

# Lua script for atomic unique enqueue: check existence, save job, push to queue
UNIQUE_ENQUEUE_SCRIPT = """
    -- KEYS[1] = job key (rq:job:{job_id})
    -- KEYS[2] = queue key (rq:queue:{queue_name})
    -- ARGV[1] = job_id
    -- ARGV[2] = push direction ("L", "R", or "N" for no push)
    -- ARGV[3] = TTL in seconds (-1 for no TTL)
    -- ARGV[4+] = field1, value1, field2, value2, ... for HSET

    -- Check if job hash already exists
    if redis.call("EXISTS", KEYS[1]) == 1 then
        return 0  -- Duplicate, reject
    end

    -- Save job hash
    if #ARGV > 3 then
        redis.call("HSET", KEYS[1], unpack(ARGV, 4))
    end

    -- Set TTL if specified
    local ttl = tonumber(ARGV[3])
    if ttl and ttl > 0 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end

    -- Push job ID to queue (skip if "N")
    if ARGV[2] == "L" then
        redis.call("LPUSH", KEYS[2], ARGV[1])
    elseif ARGV[2] == "R" then
        redis.call("RPUSH", KEYS[2], ARGV[1])
    end

    return 1  -- Success
"""

# Cache registered scripts per connection
_registered_scripts = {}


def get_unique_enqueue_script(connection):
    """Get or create the registered Lua script for unique enqueue."""
    if connection not in _registered_scripts:
        _registered_scripts[connection] = connection.register_script(UNIQUE_ENQUEUE_SCRIPT)
    return _registered_scripts[connection]


def save_unique_job(connection, queue_key, job, enqueue=True, at_front=False):
    """Atomically check uniqueness, save job, and optionally push to queue using Lua script.

    Args:
        connection: Redis connection
        queue_key (str): The Redis key for the queue
        job (Job): The job to enqueue
        enqueue (bool): Whether to push job ID to the queue. Defaults to True.
            Set to False for sync jobs that don't need to be queued.
        at_front (bool): Whether to push to front of queue

    Returns:
        bool: True if job was enqueued, False if duplicate exists

    Raises:
        DuplicateJobError: If a job with the same ID already exists
    """
    script = get_unique_enqueue_script(connection)

    hset_args = _build_hset_args(job)

    # Determine TTL (-1 means no TTL)
    ttl = job.ttl if job.ttl is not None else -1

    # Determine push direction: "L" for front, "R" for back, "N" for no push
    if not enqueue:
        push_direction = 'N'
    elif at_front:
        push_direction = 'L'
    else:
        push_direction = 'R'

    # Execute the Lua script
    result = script(
        keys=[job.key, queue_key],
        args=[job.id, push_direction, ttl] + hset_args,
    )

    if result == 0:
        raise DuplicateJobError(f"Job with ID '{job.id}' already exists")

    logger.debug('Uniquely enqueued job %s into %s', blue(job.id), green(queue_key))
    return True


# Lua script for atomic unique schedule: check existence, save job, add to scheduled registry
UNIQUE_SCHEDULE_SCRIPT = """
    -- KEYS[1] = job key (rq:job:{job_id})
    -- KEYS[2] = scheduled registry key (rq:scheduled:{queue_name})
    -- KEYS[3] = queues key (rq:queues)
    -- ARGV[1] = job_id
    -- ARGV[2] = TTL in seconds (-1 for no TTL)
    -- ARGV[3] = scheduled timestamp (UTC)
    -- ARGV[4] = queue key (rq:queue:{queue_name})
    -- ARGV[5+] = field1, value1, field2, value2, ... for HSET

    -- Check if job hash already exists
    if redis.call("EXISTS", KEYS[1]) == 1 then
        return 0  -- Duplicate, reject
    end

    -- Save job hash
    if #ARGV > 4 then
        redis.call("HSET", KEYS[1], unpack(ARGV, 5))
    end

    -- Set TTL if specified
    local ttl = tonumber(ARGV[2])
    if ttl and ttl > 0 then
        redis.call("EXPIRE", KEYS[1], ttl)
    end

    -- Add to scheduled registry sorted set
    redis.call("ZADD", KEYS[2], tonumber(ARGV[3]), ARGV[1])

    -- Register queue
    redis.call("SADD", KEYS[3], ARGV[4])

    return 1  -- Success
"""


def _build_hset_args(job):
    """Build flat list of field/value pairs from job.to_dict() for use in Lua HSET calls."""
    job_data = job.to_dict()
    hset_args = []
    for key, value in job_data.items():
        if value is not None:
            hset_args.append(key)
            if isinstance(value, bytes):
                hset_args.append(value)
            else:
                hset_args.append(str(value) if not isinstance(value, str) else value)
    return hset_args


_registered_schedule_scripts = {}


def get_unique_schedule_script(connection):
    """Get or create the registered Lua script for unique schedule."""
    if connection not in _registered_schedule_scripts:
        _registered_schedule_scripts[connection] = connection.register_script(UNIQUE_SCHEDULE_SCRIPT)
    return _registered_schedule_scripts[connection]


def schedule_unique_job(connection, queue_key, registry_key, job, scheduled_datetime):
    """Atomically check uniqueness, save job, and add to scheduled registry using Lua script.

    Args:
        connection: Redis connection
        queue_key (str): The Redis key for the queue (e.g. rq:queue:default)
        registry_key (str): The Redis key for the scheduled registry (e.g. rq:scheduled:default)
        job (Job): The job to schedule
        scheduled_datetime (datetime): The scheduled execution time

    Returns:
        bool: True if job was scheduled successfully

    Raises:
        DuplicateJobError: If a job with the same ID already exists
    """
    script = get_unique_schedule_script(connection)

    hset_args = _build_hset_args(job)

    # Determine TTL (-1 means no TTL)
    ttl = job.ttl if job.ttl is not None else -1

    # Convert datetime to UTC timestamp (same logic as ScheduledJobRegistry.schedule)
    if not scheduled_datetime.tzinfo:
        tz = timezone(timedelta(seconds=-(time.timezone if time.daylight == 0 else time.altzone)))
        scheduled_datetime = scheduled_datetime.replace(tzinfo=tz)
    timestamp = calendar.timegm(scheduled_datetime.utctimetuple())

    queues_key = 'rq:queues'

    result = script(
        keys=[job.key, registry_key, queues_key],
        args=[job.id, ttl, timestamp, queue_key] + hset_args,
    )

    if result == 0:
        raise DuplicateJobError(f"Job with ID '{job.id}' already exists")

    logger.debug('Uniquely scheduled job %s in %s', blue(job.id), green(registry_key))
    return True
