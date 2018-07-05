---
title: "RQ: Testing"
layout: docs
---

## Workers inside unit tests

You may wish to include your RQ tasks inside unit tests. However many frameworks (such as Django) use in-memory databases which do not play nicely with the default `fork()` behaviour of RQ. 

Therefore, you must use the SimpleWorker class to avoid fork();

{% highlight python %}
from redis import Redis
from rq import SimpleWorker, Queue

queue = Queue(connection=Redis())
queue.enqueue(my_long_running_job)
worker = SimpleWorker([queue], connection=queue.connection)
worker.work(burst=True)  # Runs enqueued job
# Check for result...
{% endhighlight %}


## Running Jobs in unit tests

Another solution for testing purposes is to use the `async_=False` queue
parameter, that instructs it to instantly perform the job in the same
thread instead of dispatching it to the workers. Workers are not required 
anymore.
Additionally, we can use fakeredis to mock a redis instance, so we don't have to
run a redis server separately. The instance of the fake redis server can 
be directly passed as the connection argument to the queue:

{% highlight python %}
from fakeredis import FakeStrictRedis
from rq import Queue

queue = Queue(async_=False, connection=FakeStrictRedis())
job = queue.enqueue(my_long_running_job)
assert job.is_finished
{% endhighlight %}
