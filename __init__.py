from rdb import conn
from pickle import loads, dumps

def queue_daemon(app, queue_keys, rv_ttl=500):
    """Simple implementation of a Redis queue worker, based on
    http://flask.pocoo.org/snippets/73/

    Will listen endlessly on the given queue keys.
    """
    while True:
        msg = conn.blpop(queue_keys)
        func, key, args, kwargs = loads(msg[1])
        try:
            rv = func(*args, **kwargs)
        except Exception, e:
            rv = e
        if rv is not None:
            p = conn.pipeline()
            conn.set(key, dumps(rv))
            conn.expire(key, rv_ttl)
            p.execute()
