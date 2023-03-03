---
title: "RQ: Connections"
layout: docs
---

Although RQ features the `use_connection()` command for convenience, it
is deprecated, since it pollutes the global namespace.  Instead, prefer explicit
connection management using the `with Connection(...):` context manager, or
pass in Redis connection references to queues directly.


## Single Redis connection (easy)

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px" src="/img/warning.png" />
    <strong>Note:</strong>
    <p>
        The use of <code>use_connection</code> is deprecated.
        Please don't use <code>use_connection</code> in your scripts.
        Instead, use explicit connection management.
    </p>
</div>

In development mode, to connect to a default, local Redis server:

```python
from rq import use_connection
use_connection()
```

In production, to connect to a specific Redis server:

```python
from redis import Redis
from rq import use_connection

redis = Redis('my.host.org', 6789, password='secret')
use_connection(redis)
```

Be aware of the fact that `use_connection` pollutes the global namespace.  It
also implies that you can only ever use a single connection.


## Multiple Redis connections

However, the single connection pattern facilitates only those cases where you
connect to a single Redis instance, and where you affect global context (by
replacing the existing connection with the `use_connection()` call).  You can
only use this pattern when you are in full control of your web stack.

In any other situation, or when you want to use multiple connections, you
should use `Connection` contexts or pass connections around explicitly.


### Explicit connections (precise, but tedious)

Each RQ object instance (queues, workers, jobs) has a `connection` keyword
argument that can be passed to the constructor.  Using this, you don't need to
use `use_connection()`.  Instead, you can create your queues like this:

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

This approach is very precise, but rather verbose, and therefore, tedious.


### Connection contexts (precise and concise)

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
SENTINEL: {'INSTANCES':[('remote.host1.org', 26379), ('remote.host2.org', 26379), ('remote.host3.org', 26379)],
           'SOCKET_TIMEOUT': None,
           'PASSWORD': 'secret',
           'DB': 2,
           'MASTER_NAME': 'master'}
```


### Timeout

To avoid potential issues with hanging Redis commands, specifically the blocking `BLPOP` command,
RQ automatically sets a `socket_timeout` value that is 10 seconds higher than the `default_worker_ttl`.

If you prefer to manually set the `socket_timeout` value,
make sure that the value being set is higher than the `default_worker_ttl` (which is 420 by default).

```python
from redis import Redis
from rq import Queue

conn = Redis('localhost', 6379, socket_timeout=500)
q = Queue(connection=conn)
```

Setting a `socket_timeout` with a lower value than the `default_worker_ttl` will cause a `TimeoutError`
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
