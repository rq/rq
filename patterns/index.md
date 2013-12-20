---
title: "RQ: Using RQ on Heroku"
layout: patterns
---


## Using RQ on Heroku

To setup RQ on [Heroku][1], first add it to your
`requirements.txt` file:

    redis==2.4.11
    rq==0.3.4

Create a file called `run-worker.py` with the following content (assuming you
are using [Redis To Go][2] with Heroku):

{% highlight python %}
import os
import urlparse
from redis import Redis
from rq import Worker, Queue, Connection

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISTOGO_URL')
if not redis_url:
    raise RuntimeError('Set up Redis To Go first.')

urlparse.uses_netloc.append('redis')
url = urlparse.urlparse(redis_url)
conn = Redis(host=url.hostname, port=url.port, db=0, password=url.password)

with Connection(conn):
    worker = Worker(map(Queue, listen))
    worker.work()
{% endhighlight %}

Than, add the command to your `Procfile`:

    worker: python -u run-worker.py

Now, all you have to do is spin up a worker:

{% highlight console %}
$ heroku scale worker=1
{% endhighlight %}


## Putting RQ under foreman

[Foreman][3] is probably the process manager you use when you host your app on
Heroku, or just because it's a pretty friendly tool to use in development.

When using RQ under `foreman`, you may experience that the workers are a bit
quiet sometimes.  This is because of Python buffering the output, so `foreman`
cannot (yet) echo it.  Here's a related [Wiki page][4].

Just change the way you run your worker process, by adding the `-u` option (to
force stdin, stdout and stderr to be totally unbuffered):

    worker: python -u run-worker.py

[1]: https://heroku.com
[2]: https://devcenter.heroku.com/articles/redistogo
[3]: http://supervisord.org/
[4]: https://github.com/ddollar/foreman/wiki/Missing-Output
