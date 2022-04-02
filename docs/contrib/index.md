---
title: "RQ: Simple job queues for Python"
layout: contrib
---

This document describes how RQ works internally when enqueuing or dequeueing.


## Enqueueing internals

Whenever a function call gets enqueued, RQ does two things:

* It creates a job instance representing the delayed function call and persists
  it in a Redis [hash][h]; and
* It pushes the given job's ID onto the requested Redis queue.

All jobs are stored in Redis under the `rq:job:` prefix, for example:

    rq:job:55528e58-9cac-4e05-b444-8eded32e76a1

The keys of such a job [hash][h] are:

    created_at  => '2012-02-13 14:35:16+0000'
    enqueued_at => '2012-02-13 14:35:16+0000'
    origin      => 'default'
    data        => <pickled representation of the function call>
    description => "count_words_at_url('http://nvie.com')"

Depending on whether or not the job has run successfully or has failed, the
following keys are available, too:

    ended_at    => '2012-02-13 14:41:33+0000'
    result      => <pickled return value>
    exc_info    => <exception information>

[h]: http://redis.io/topics/data-types#hashes


## Dequeueing internals

Whenever a dequeue is requested, an RQ worker does two things:

* It pops a job ID from the queue, and fetches the job data belonging to that
  job ID;
* It starts executing the function call.
* If the job succeeds, its return value is written to the `result` hash key and
  the hash itself is expired after 500 seconds; or
* If the job fails, the exception information is written to the `exc_info`
  hash key and the job ID is pushed onto the `failed` queue.


## Cancelling jobs

Any job ID that is encountered by a worker for which no job hash is found in
Redis is simply ignored.  This makes it easy to cancel jobs by simply removing
the job hash.  In Python:

```python
    from rq import cancel_job
    cancel_job('2eafc1e6-48c2-464b-a0ff-88fd199d039c')
```

Note that it is irrelevant on which queue the job resides.  When a worker
eventually pops the job ID from the queue and notes that the Job hash does not
exist (anymore), it simply discards the job ID and continues with the next.

