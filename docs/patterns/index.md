---
title: "RQ: Using RQ on Heroku"
layout: patterns
---

## Using RQ on Heroku

To setup RQ on [Heroku][1], first add it to your
`requirements.txt` file:

    redis>=3
    rq>=0.13

Create a file called `run-worker.py` with the following content (assuming you
are using [Redis To Go][2] with Heroku):

{% highlight python %}
import os
import urlparse
from redis import Redis
from rq import Queue, Connection
from rq.worker import HerokuWorker as Worker

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISTOGO_URL')
if not redis_url:
    raise RuntimeError('Set up Redis To Go first.')

urlparse.uses_netloc.append('redis')
url = urlparse.urlparse(redis_url)
conn = Redis(host=url.hostname, port=url.port, db=0, password=url.password)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
{% endhighlight %}

Then, add the command to your `Procfile`:

    worker: python -u run-worker.py

Now, all you have to do is spin up a worker:

{% highlight console %}
$ heroku scale worker=1
{% endhighlight %}

If you are using [Heroku Redis][5]) you might need to change the Redis connection as follows:

{% highlight console %}
conn = redis.Redis(
    host=host,
    password=password,
    port=port,
    ssl=True,
    ssl_cert_reqs=None
)
{% endhighlight %}

and for using the cli:

{% highlight console %}
rq info --config rq_conf
{% endhighlight %}{% endhighlight %}

Where the rq_conf.py file looks like:
{% highlight console %}
REDIS_HOST = "host"
REDIS_PORT = port
REDIS_PASSWORD = "password"
REDIS_SSL = True
REDIS_SSL_CA_CERTS = None
REDIS_DB = 0
REDIS_SSL_CERT_REQS = None
{% endhighlight %}{% endhighlight %}

## Putting RQ under foreman

[Foreman][3] is probably the process manager you use when you host your app on
Heroku, or just because it's a pretty friendly tool to use in development.

When using RQ under `foreman`, you may experience that the workers are a bit quiet sometimes. This is because of Python buffering the output, so `foreman`
cannot (yet) echo it. Here's a related [Wiki page][4].

Just change the way you run your worker process, by adding the `-u` option (to
force stdin, stdout and stderr to be totally unbuffered):

    worker: python -u run-worker.py

[1]: https://heroku.com
[2]: https://devcenter.heroku.com/articles/redistogo
[3]: https://github.com/ddollar/foreman
[4]: https://github.com/ddollar/foreman/wiki/Missing-Output
[5]: https://elements.heroku.com/addons/heroku-redis
