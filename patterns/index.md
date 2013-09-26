---
title: "RQ: Using RQ on Heroku"
layout: patterns
---


## Using RQ on Heroku

To setup RQ on [Heroku](https://heroku.com/), first add it to your
`requirements.txt` file:

    redis==2.4.11
    rq==0.3.4

Create a file called `run-worker.py` with the following content (assuming you
are using [Redis To Go](https://devcenter.heroku.com/articles/redistogo) with
Heroku):

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

    worker: python run-worker.py

Now, all you have to do is spin up a worker:

{% highlight console %}
$ heroku scale worker=1
{% endhighlight %}
