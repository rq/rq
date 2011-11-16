---
title: "RQ: Simple job queues for Python"
layout: default
---

# RQ

[RQ](http://git.io/rq) is a simple job queue library for Python.

It is really simple:

{% highlight python %}
from rq import Queue

q = Queue()
q.enqueue(fib, 36)
{% endhighlight %}

Which is equivalent to calling

{% highlight python %}
fib(36)
{% endhighlight %}

