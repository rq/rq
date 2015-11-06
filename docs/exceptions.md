---
title: "RQ: Exceptions"
layout: docs
---

Jobs can fail due to exceptions occurring.  When your RQ workers run in the
background, how do you get notified of these exceptions?

## Default: the `failed` queue

The default safety net for RQ is the `failed` queue.  Every job that fails
execution is stored in here, along with its exception information (type,
value, traceback).  While this makes sure no failing jobs "get lost", this is
of no use to get notified pro-actively about job failure.


## Custom exception handlers

Starting from version 0.3.1, RQ supports registering custom exception
handlers.  This makes it possible to replace the default behaviour (sending
the job to the `failed` queue) altogether, or to take additional steps when an
exception occurs.

To do this, register your custom exception handler to an RQ worker as follows:

{% highlight python %}
with Connection():
    q = Queue()
    w = Worker([q])
    worker.push_exc_handler(my_handler)
    worker.work()
{% endhighlight %}

While the exception handlers are a FILO stack, most times you only want to
register a single handler. Therefore, for convenience, you can pass it to the
constructor directly, too:

{% highlight python %}
with Connection():
    w = Worker([q], exception_handlers=[my_handler, self.move_to_failed_queue])
    ...
{% endhighlight %}

The handler itself is a function that takes the following parameters: `job`,
`exc_type`, `exc_value` and `traceback`:

{% highlight python %}
def my_handler(job, exc_type, exc_value, traceback):
    # do custom things here
    # for example, write the exception info to a DB
    ...
{% endhighlight %}

You might also see the three exception arguments encoded as:

{% highlight python %}
def my_handler(job, *exc_info):
    # do custom things here
    ...
{% endhighlight %}


## Chaining exception handlers

The handler itself is responsible for deciding whether or not the exception
handling is done, or should fall through to the next handler on the stack.
The handler can indicate this by returning a boolean.  `False` means stop
processing exceptions, `True` means continue and fall through to the next
exception handler on the stack.

It's important to know for implementors that, by default, when the handler
doesn't have an explicit return value (thus `None`), this will be interpreted
as `True` (i.e.  continue with the next handler).

To replace the default behaviour (i.e. moving the job to the `failed` queue),
use a custom exception handler that doesn't fall through, for example:

{% highlight python %}
def black_hole(job, *exc_info):
    return False
{% endhighlight %}
