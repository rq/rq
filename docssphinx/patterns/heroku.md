# Heroku

## Using RQ on Heroku

To setup RQ on [Heroku](https://heroku.com), first add it to your
`requirements.txt` file:

```
redis>=3
rq>=0.13
```

Create a file called `run-worker.py` with the following content
(assuming you are using [Redis To
Go](https://devcenter.heroku.com/articles/redistogo) with Heroku):

```python
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
```

Then, add the command to your `Procfile`:

```
worker: python -u run-worker.py
```

Now, all you have to do is spin up a worker:

```console
$ heroku scale worker=1
```

## Putting RQ under foreman

[Foreman](https://github.com/ddollar/foreman) is probably the process
manager you use when you host your app on Heroku, or just because it’s a
pretty friendly tool to use in development.

When using RQ under `foreman`, you may experience that the workers are
a bit quiet sometimes. This is because of Python buffering the output,
so `foreman` cannot (yet) echo it. Here’s a related [Wiki
page](https://github.com/ddollar/foreman/wiki/Missing-Output).

Just change the way you run your worker process, by adding the `-u`
option (to force stdin, stdout and stderr to be totally unbuffered):

```
worker: python -u run-worker.py
```
