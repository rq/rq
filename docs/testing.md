---
title: "RQ: Testing"
layout: docs
---

## Workers inside unit tests

You may wish to include your rq tasks inside unit tests. However many frameworks (such as Django) use in-memory databases which do not play nicely with the default `fork()` behaviour of rq. 

Therefore, you must use the SimpleWorker class to avoid fork();

{% highlight python %}
from rq import SimpleWorker
SimpleWorker()
{% endhighlight %}

To run your tasks inside a unit test, you might want to use something like;

{% highlight python %}
from rq import SimpleWorker

def run_task_queue(queues=None):
    qs = map(Queue, queues) or [Queue()]
    w = SimpleWorker(qs, fork=False)
    w.work(burst=True)
{% endhighlight %}
