from werkzeug.local import LocalStack

class NoRedisConnectionException(Exception):
    pass

_conn = LocalStack()

def push_connection(redis_conn):
    _conn.push(redis_conn)

def pop_connection():
    return _conn.pop()

def current_connection():
    conn = _conn.top
    if conn is None:
        raise NoRedisConnectionException('Connect to Redis first.')
    return conn
