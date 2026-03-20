import logging

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


def persist_unique_job(connection, queue_key, job, enqueue=True, at_front=False):
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

    # Build the HSET field/value pairs from job.to_dict()
    job_data = job.to_dict()
    hset_args = []
    for key, value in job_data.items():
        if value is not None:
            hset_args.append(key)
            # Convert bytes to string for Redis
            if isinstance(value, bytes):
                hset_args.append(value)
            else:
                hset_args.append(str(value) if not isinstance(value, str) else value)

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
