import sys


def is_python_version(*versions):
    for version in versions:
        if (sys.version_info[0] == version[0] and sys.version_info >= version):
            return True
    return False


PY2 = sys.version_info[0] == 2

# Python 3.x and up
text_type = str
string_types = (str,)


def as_text(v):
    if v is None:
        return None
    elif isinstance(v, bytes):
        return v.decode('utf-8')
    elif isinstance(v, str):
        return v
    else:
        raise ValueError('Unknown type %r' % type(v))


def decode_redis_hash(h):
    return dict((as_text(k), h[k]) for k in h)
