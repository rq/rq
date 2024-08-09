---
title: "RQ: Connections"
layout: docs
---

### The connection parameter

Each RQ object (queues, workers, jobs) has a `connection` keyword
argument that can be passed to the constructor - this is the recommended way of handling connections.

```python
from redis import Redis
from rq import Queue

redis = Redis('localhost', 6789)
q = Queue(connection=redis)
```

This pattern allows for different connections to be passed to different objects:

```python
from rq import Queue
from redis import Redis

conn1 = Redis('localhost', 6379)
conn2 = Redis('remote.host.org', 9836)

q1 = Queue('foo', connection=conn1)
q2 = Queue('bar', connection=conn2)
```

Every job that is enqueued on a queue will know what connection it belongs to.
The same goes for the workers.


### Connection contexts (precise and concise)

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>
        The use of <code>Connection</code> context manager is deprecated.
        Please don't use <code>Connection</code> in your scripts.
        Instead, use explicit connection management.
    </p>
</div>

There is a better approach if you want to use multiple connections, though.
Each RQ object instance, upon creation, will use the topmost Redis connection
on the RQ connection stack, which is a mechanism to temporarily replace the
default connection to be used.

An example will help to understand it:

```python
from rq import Queue, Connection
from redis import Redis

with Connection(Redis('localhost', 6379)):
    q1 = Queue('foo')
    with Connection(Redis('remote.host.org', 9836)):
        q2 = Queue('bar')
    q3 = Queue('qux')

assert q1.connection != q2.connection
assert q2.connection != q3.connection
assert q1.connection == q3.connection
```

You can think of this as if, within the `Connection` context, every newly
created RQ object instance will have the `connection` argument set implicitly.
Enqueueing a job with `q2` will enqueue it in the second (remote) Redis
backend, even when outside of the connection context.


### Pushing/popping connections

If your code does not allow you to use a `with` statement, for example, if you
want to use this to set up a unit test, you can use the `push_connection()` and
`pop_connection()` methods instead of using the context manager.

```python
import unittest
from rq import Queue
from rq import push_connection, pop_connection

class MyTest(unittest.TestCase):
    def setUp(self):
        push_connection(Redis())

    def tearDown(self):
        pop_connection()

    def test_foo(self):
        """Any queues created here use local Redis."""
        q = Queue()
```

### Sentinel support

To use redis sentinel, you must specify a dictionary in the configuration file.
Using this setting in conjunction with the systemd or docker containers with the
automatic restart option allows workers and RQ to have a fault-tolerant connection to the redis.

```python
SENTINEL: {
    'INSTANCES':[('remote.host1.org', 26379), ('remote.host2.org', 26379), ('remote.host3.org', 26379)],
    'MASTER_NAME': 'master',
    'DB': 2,
    'USERNAME': 'redis-user',
    'PASSWORD': 'redis-secret',
    'SOCKET_TIMEOUT': None,
    'CONNECTION_KWARGS': {  # Eventual addition Redis connection arguments
        'ssl_ca_path': None,
    },
    'SENTINEL_KWARGS': {    # Eventual Sentinels connections arguments
        'username': 'sentinel-user',
        'password': 'sentinel-secret',
    },
}
```


### Timeout

To avoid potential issues with hanging Redis commands, specifically the blocking `BLPOP` command,
RQ automatically sets a `socket_timeout` value that is 10 seconds higher than the `dequeue_timeout`. The `dequeue_timeout` is computed as 15 seconds shorter than the `worker_ttl` value.

Here are the following computed timeout values if you were not to adjust anything.

| Setting              | Default Value |
|:---------------------|:-------------:|
| `worker_ttl`         |      420      |
| `connection_timeout` |      415      |
| `dequeue_timeout`    |      405      |
|                      |               |



If you prefer to manually set the `socket_timeout` value,
make sure that the value being set is higher than the `dequeue_timeout` (which is 405 by default).

```python
from redis import Redis
from rq import Queue

conn = Redis('localhost', 6379, socket_timeout=500)
q = Queue(connection=conn)
```

Setting a `socket_timeout` with a lower value than the `dequeue_timeout` will cause a `TimeoutError`
since it will interrupt the worker while it gets new jobs from the queue.


### Encoding / Decoding

The encoding and decoding of Redis objects occur in multiple locations within the codebase,
which means that the `decode_responses=True` argument of the Redis connection is not currently supported.

```python
from redis import Redis
from rq import Queue

conn = Redis(..., decode_responses=True) # This is not supported
q = Queue(connection=conn)
```
