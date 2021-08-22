---
title: "RQ: Job Registries"
layout: docs
---

Each queue maintains a set of Job Registries:
* `StartedJobRegistry` Holds currently executing jobs. Jobs are added right before they are 
executed and removed right after completion (success or failure).
* `FinishedJobRegistry` Holds successfully completed jobs.
* `FailedJobRegistry` Holds jobs that have been executed, but didn't finish successfully.
* `DeferredJobRegistry` Holds deferred jobs (jobs that depend on another job and are waiting for that 
job to finish).
* `ScheduledJobRegistry` Holds scheduled jobs.
* `CanceledJobRegistry` Holds canceled jobs.

You can get the number of jobs in a registry, the ids of the jobs in the registry, and more. 
Below is an example using a `StartedJobRegistry`.
```python
import time
from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry
from somewhere import count_words_at_url

redis = Redis()
queue = Queue(connection=redis)
job = queue.enqueue(count_words_at_url, 'http://nvie.com')

# get StartedJobRegistry by queue
registry = StartedJobRegistry(queue=queue)

# or get StartedJobRegistry by queue name and connection
registry2 = StartedJobRegistry(name='my_queue', connection=redis)

# sleep for a moment while job is taken off the queue
time.sleep(0.1)

print('Queue associated with the registry: %s' % registry.get_queue())
print('Number of jobs in registry %s' % registry.count)

# get the list of ids for the jobs in the registry
print('IDs in registry %s' % registry.get_job_ids())

# test if a job is in the registry using the job instance or job id
print('Job in registry %s' % (job in registry))
print('Job in registry %s' % (job.id in registry))
```

_New in version 1.2.0_

You can quickly access job registries from `Queue` objects.

```python
from redis import Redis
from rq import Queue

redis = Redis()
queue = Queue(connection=redis)

queue.started_job_registry  # Returns StartedJobRegistry
queue.deferred_job_registry   # Returns DeferredJobRegistry
queue.finished_job_registry  # Returns FinishedJobRegistry
queue.failed_job_registry  # Returns FailedJobRegistry
queue.scheduled_job_registry  # Returns ScheduledJobRegistry
```

## Removing Jobs

_New in version 1.2.0_

To remove a job from a job registry, use `registry.remove()`. This is useful
when you want to manually remove jobs from a registry, such as deleting failed
jobs before they expire from `FailedJobRegistry`.

```python
from redis import Redis
from rq import Queue
from rq.registry import FailedJobRegistry

redis = Redis()
queue = Queue(connection=redis)
registry = FailedJobRegistry(queue=queue)

# This is how to remove a job from a registry
for job_id in registry.get_job_ids():
    registry.remove(job_id)

# If you want to remove a job from a registry AND delete the job,
# use `delete_job=True`
for job_id in registry.get_job_ids():
    registry.remove(job_id, delete_job=True)
```
