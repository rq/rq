---
title: "RQ: Job Registries"
layout: docs
---

Each queue maintains a set of Job Registries:
* `StartedJobRegistry` Holds currently executing jobs. Jobs are added right before they are 
executed and removed right after completion (success or failure).
* `FinishedJobRegistry` Jobs are added to this registry after they have been successfully completed.
* `FailedJobRegistry` Jobs that have failed.
* `DeferredJobRegistry` Deferred jobs (jobs that depend on another job and are waiting for that 
job to finish).

You can get the number of jobs in a registry, the ids of the jobs in the registry, and more. 
Below is an example using a `StartedJobRegistry`.
```python
import time
from redis import Redis
from rq import Queue
from rq.registry import StartedJobRegistry
from somewhere import count_words_at_url

conn = Redis()
queue = Queue(connection=conn)
job = queue.enqueue(count_words_at_url, 'http://nvie.com')

# get StartedJobRegistry by queue
registry = StartedJobRegistry(queue=queue)

# or get StartedJobRegistry by connection
registry = StartedJobRegistry(connection=conn)

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