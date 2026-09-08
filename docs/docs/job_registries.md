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

## Purging a Registry

To clear out a whole registry at once, use `registry.purge()`. It removes every entry and
deletes the jobs themselves, working in chunks so that a large registry doesn't have to be
loaded into memory. It returns the number of registry entries it removed.

```python
from redis import Redis
from rq import Queue

redis = Redis()
queue = Queue(connection=redis)

# Delete every failed job of this queue
queue.failed_job_registry.purge()

# Clear the registry but leave the job data to expire through its own TTL.
# Much faster on a very large registry, since the jobs are never fetched.
queue.failed_job_registry.purge(delete_jobs=False)
```

`Queue.purge_registries()` does the same for several registries in one call, returning the
number of entries removed from each:

```python
queue.purge_registries(failed=True, finished=True)
# {'failed': 373, 'finished': 12}
```

Only the registries holding jobs that will never run again can be purged this way: `failed`,
`finished` and `canceled`. Deferred and scheduled jobs are still waiting to run, and started
jobs may be executing right now, so those registries are left out on purpose.

### Purging Registries via CLI

The same thing from the command line, through `rq empty`:

```console
# Delete every failed job of the myqueue queue
rq empty --failed myqueue

# Clear the failed, finished and canceled registries of two queues
rq empty --registries myqueue myotherqueue

# ...of every queue
rq empty --registries --all

# Clear the registry entries but leave the job data to expire through its own TTL
rq empty --failed --keep-jobs myqueue
```

Without any of these options `rq empty` behaves as it always has and empties the queue itself,
leaving the registries alone. The two can be combined:

```console
# Empty the queue AND its failed, finished and canceled registries
rq empty --queued --registries myqueue
```
