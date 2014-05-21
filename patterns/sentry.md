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
from rq.contrib.sentry import register_sentry

client = Client('your sentry DSN here')
register_sentry(client, worker)
{% endhighlight %}

Where `worker` is your RQ worker instance.  After that, call `worker.work(...)`
to start the worker.  All exceptions that occur are reported to Sentry
automatically.

<div class="warning" style="margin-top: 20px">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="{{site.baseurl}}img/warning.png" />
    <strong>Note:</strong>
    <p>
      Error delivery to Sentry is known to be unreliable with RQ when using
      async transports (the default is).  So you are encouraged to use the
      <code>sync+https://</code> or <code>requests+https://</code> versions of
      the Sentry DSN in RQ.
    </p>
</div>

Read more on RQ's [custom exception handling](/docs/exceptions/) capabilities.
