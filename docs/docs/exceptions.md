---
title: "RQ: Exceptions"
layout: docs
---

Jobs can fail due to exceptions occurring.  When your RQ workers run in the
background, how do you get notified of these exceptions?

## Default: the `FailedJobRegistry`

The default safety net for RQ is the `FailedJobRegistry`. Every job that doesn't
execute successfully is stored here, along with its exception information (type,
value, traceback). While this makes sure no failing jobs "get lost", this is
of no use to get notified pro-actively about job failure.


## Custom Exception Handlers

RQ supports registering custom exception handlers. This makes it possible to
inject your own error handling logic to your workers.

This is how you register custom exception handler(s) to an RQ worker:

```python
from exception_handlers import foo_handler, bar_handler

w = Worker([q], exception_handlers=[foo_handler, bar_handler])
```

The handler itself is a function that takes the following parameters: `job`,
`exc_type`, `exc_value` and `traceback`:

```python
def my_handler(job, exc_type, exc_value, traceback):
    # do custom things here
    # for example, write the exception info to a DB

```

You might also see the three exception arguments encoded as:

```python
def my_handler(job, *exc_info):
    # do custom things here
```

{% highlight python %}
from exception_handlers import foo_handler

w = Worker([q], exception_handlers=[foo_handler],
           disable_default_exception_handler=True)
{% endhighlight %}


## Chaining Exception Handlers

The handler itself is responsible for deciding whether or not the exception
handling is done, or should fall through to the next handler on the stack.
The handler can indicate this by returning a boolean. `False` means stop
processing exceptions, `True` means continue and fall through to the next
exception handler on the stack.

It's important to know for implementors that, by default, when the handler
doesn't have an explicit return value (thus `None`), this will be interpreted
as `True` (i.e.  continue with the next handler).

To prevent the next exception handler in the handler chain from executing,
use a custom exception handler that doesn't fall through, for example:

```python
def black_hole(job, *exc_info):
    return False
```
