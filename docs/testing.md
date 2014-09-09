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
worker = SimpleWorker([queue])
worker.work(burst=True)  # Runs enqueued job
# Check for result...
{% endhighlight %}
