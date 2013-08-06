import sys


def is_python_version(*versions):
    for version in versions:
        if (sys.version_info[0] == version[0] and
                sys.version_info >= version):
            return True
    return False


# functools.total_ordering is only available from Python 2.7 and 3.2
if is_python_version((2, 7), (3, 2)):
    from functools import total_ordering
else:
    def total_ordering(cls):  # noqa
        """Class decorator that fills in missing ordering methods"""
        convert = {
            '__lt__': [('__gt__', lambda self, other: other < self),
                    ('__le__', lambda self, other: not other < self),
                    ('__ge__', lambda self, other: not self < other)],
            '__le__': [('__ge__', lambda self, other: other <= self),
                    ('__lt__', lambda self, other: not other <= self),
                    ('__gt__', lambda self, other: not self <= other)],
            '__gt__': [('__lt__', lambda self, other: other > self),
                    ('__ge__', lambda self, other: not other > self),
                    ('__le__', lambda self, other: not self > other)],
            '__ge__': [('__le__', lambda self, other: other >= self),
                    ('__gt__', lambda self, other: not other >= self),
                    ('__lt__', lambda self, other: not self >= other)]
        }
        roots = set(dir(cls)) & set(convert)
        if not roots:
            raise ValueError('must define at least one ordering operation: < > <= >=')  # noqa
        root = max(roots)       # prefer __lt__ to __le__ to __gt__ to __ge__
        for opname, opfunc in convert[root]:
            if opname not in roots:
                opfunc.__name__ = opname
                opfunc.__doc__ = getattr(int, opname).__doc__
                setattr(cls, opname, opfunc)
        return cls


PY2 = sys.version_info[0] < 3

if PY2:
    string_types = (str, unicode)
    text_type = unicode

    def as_text(v):
        return v

    def decode_redis_hash(h):
        return h

else:
    string_types = (str,)
    text_type = str

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
