---
title: "RQ: Sending exceptions to Sentry"
layout: patterns
---

## Sending exceptions to Sentry

[Sentry](https://www.getsentry.com/) is a popular exception gathering service
that RQ supports integrating with since version 0.3.1, through its custom
exception handlers.

<div class="warning" style="margin-top: 20px">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="{{site.baseurl}}img/warning.png" />
    <strong>Note:</strong>
    <p>
      This feature is deprecated. You are encouraged to use <a href="https://docs.sentry.io/platforms/python/rq/">Sentry's own RQ integration</a> in their new SDK.
    </p>
</div>

RQ includes a convenience function that registers your existing Sentry client
to send all exceptions to.

An example:

{% highlight python %}
from raven import Client
from raven.transport.http import HTTPTransport
from rq.contrib.sentry import register_sentry

client = Client('<YOUR_DSN>', transport=HTTPTransport)
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
      <code>HTTPTransport</code> or <code>RequestsHTTPTransport</code> when
      creating your client.  See the code sample above, or the <a
      href="http://raven.readthedocs.org/en/latest/transports/index.html">Raven
      documentation</a>.
    </p>
    <p>
      For more info, see the
      <a href="http://raven.readthedocs.org/en/latest/transports/index.html#transports">Raven docs</a>.
    </p>
</div>

Read more on RQ's [custom exception handling](/docs/exceptions/) capabilities.
