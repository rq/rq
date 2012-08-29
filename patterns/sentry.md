---
title: "RQ: Sending exceptions to Sentry"
layout: patterns
---

## Sending exceptions to Sentry

[Sentry](https://www.getsentry.com/) is a popular exception gathering service
that RQ supports integrating with since version 0.3.1, through its custom
exception handlers.

RQ includes a convenience function that registers your existing Sentry client
to send all exceptions to.

An example:

{% highlight python %}
from raven import Client

client = Client('your sentry DSN here')
register_sentry(client, worker)
{% endhighlight %}

Where `worker` is your RQ worker instance.  After that, call `worker.work(...)`
to start the worker.  All exceptions that occur are reported to Sentry
automatically.

Read more on RQ's [custom exception handling]() capabilities.
