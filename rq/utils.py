# -*- coding: utf-8 -*-
"""
Miscellaneous helper functions.

The formatter for ANSI colored console output is heavily based on Pygments
terminal colorizing code, originally by Georg Brandl.
"""
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import calendar
import datetime
import importlib
import logging
import numbers
import sys
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable

from .compat import as_text, is_python_version, string_types
from .exceptions import TimeoutFormatError


class _Colorizer(object):
    def __init__(self):
        esc = "\x1b["

        self.codes = {}
        self.codes[""] = ""
        self.codes["reset"] = esc + "39;49;00m"

        self.codes["bold"] = esc + "01m"
        self.codes["faint"] = esc + "02m"
        self.codes["standout"] = esc + "03m"
        self.codes["underline"] = esc + "04m"
        self.codes["blink"] = esc + "05m"
        self.codes["overline"] = esc + "06m"

        dark_colors = ["black", "darkred", "darkgreen", "brown", "darkblue",
                       "purple", "teal", "lightgray"]
        light_colors = ["darkgray", "red", "green", "yellow", "blue",
                        "fuchsia", "turquoise", "white"]

        x = 30
        for d, l in zip(dark_colors, light_colors):
            self.codes[d] = esc + "%im" % x
            self.codes[l] = esc + "%i;01m" % x
            x += 1

        del d, l, x

        self.codes["darkteal"] = self.codes["turquoise"]
        self.codes["darkyellow"] = self.codes["brown"]
        self.codes["fuscia"] = self.codes["fuchsia"]
        self.codes["white"] = self.codes["bold"]

        if hasattr(sys.stdout, "isatty"):
            self.notty = not sys.stdout.isatty()
        else:
            self.notty = True

    def reset_color(self):
        return self.codes["reset"]

    def colorize(self, color_key, text):
        if self.notty:
            return text
        else:
            return self.codes[color_key] + text + self.codes["reset"]

    def ansiformat(self, attr, text):
        """
        Format ``text`` with a color and/or some attributes::

            color       normal color
            *color*     bold color
            _color_     underlined color
            +color+     blinking color
        """
        result = []
        if attr[:1] == attr[-1:] == '+':
            result.append(self.codes['blink'])
            attr = attr[1:-1]
        if attr[:1] == attr[-1:] == '*':
            result.append(self.codes['bold'])
            attr = attr[1:-1]
        if attr[:1] == attr[-1:] == '_':
            result.append(self.codes['underline'])
            attr = attr[1:-1]
        result.append(self.codes[attr])
        result.append(text)
        result.append(self.codes['reset'])
        return ''.join(result)


colorizer = _Colorizer()


def make_colorizer(color):
    """Creates a function that colorizes text with the given color.

    For example:

        green = make_colorizer('darkgreen')
        red = make_colorizer('red')

    Then, you can use:

        print "It's either " + green('OK') + ' or ' + red('Oops')
    """
    def inner(text):
        return colorizer.colorize(color, text)
    return inner


class ColorizingStreamHandler(logging.StreamHandler):

    levels = {
        logging.WARNING: make_colorizer('darkyellow'),
        logging.ERROR: make_colorizer('darkred'),
        logging.CRITICAL: make_colorizer('darkred'),
    }

    def __init__(self, exclude=None, *args, **kwargs):
        self.exclude = exclude
        if is_python_version((2, 6)):
            logging.StreamHandler.__init__(self, *args, **kwargs)
        else:
            super(ColorizingStreamHandler, self).__init__(*args, **kwargs)

    @property
    def is_tty(self):
        isatty = getattr(self.stream, 'isatty', None)
        return isatty and isatty()

    def format(self, record):
        message = logging.StreamHandler.format(self, record)
        if self.is_tty:
            colorize = self.levels.get(record.levelno, lambda x: x)

            # Don't colorize any traceback
            parts = message.split('\n', 1)
            parts[0] = " ".join([parts[0].split(" ", 1)[0], colorize(parts[0].split(" ", 1)[1])])

            message = '\n'.join(parts)

        return message


def import_attribute(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, attribute = name.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, attribute)


def utcnow():
    return datetime.datetime.utcnow()


_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def utcformat(dt):
    return dt.strftime(as_text(_TIMESTAMP_FORMAT))


def utcparse(string):
    try:
        return datetime.datetime.strptime(string, _TIMESTAMP_FORMAT)
    except ValueError:
        # This catches any jobs remain with old datetime format
        return datetime.datetime.strptime(string, '%Y-%m-%dT%H:%M:%SZ')


def first(iterable, default=None, key=None):
    """
    Return first element of `iterable` that evaluates true, else return None
    (or an optional default value).

    >>> first([0, False, None, [], (), 42])
    42

    >>> first([0, False, None, [], ()]) is None
    True

    >>> first([0, False, None, [], ()], default='ohai')
    'ohai'

    >>> import re
    >>> m = first(re.match(regex, 'abc') for regex in ['b.*', 'a(.*)'])
    >>> m.group(1)
    'bc'

    The optional `key` argument specifies a one-argument predicate function
    like that used for `filter()`.  The `key` argument, if supplied, must be
    in keyword form.  For example:

    >>> first([1, 1, 3, 4, 5], key=lambda x: x % 2 == 0)
    4

    """
    if key is None:
        for el in iterable:
            if el:
                return el
    else:
        for el in iterable:
            if key(el):
                return el

    return default


def is_nonstring_iterable(obj):
    """Returns whether the obj is an iterable, but not a string"""
    return isinstance(obj, Iterable) and not isinstance(obj, string_types)


def ensure_list(obj):
    """
    When passed an iterable of objects, does nothing, otherwise, it returns
    a list with just that object in it.
    """
    return obj if is_nonstring_iterable(obj) else [obj]


def current_timestamp():
    """Returns current UTC timestamp"""
    return calendar.timegm(datetime.datetime.utcnow().utctimetuple())


def enum(name, *sequential, **named):
    values = dict(zip(sequential, range(len(sequential))), **named)

    # NOTE: Yes, we *really* want to cast using str() here.
    # On Python 2 type() requires a byte string (which is str() on Python 2).
    # On Python 3 it does not matter, so we'll use str(), which acts as
    # a no-op.
    return type(str(name), (), values)


def backend_class(holder, default_name, override=None):
    """Get a backend class using its default attribute name or an override"""
    if override is None:
        return getattr(holder, default_name)
    elif isinstance(override, string_types):
        return import_attribute(override)
    else:
        return override


def str_to_date(date_str):
    if date_str is None:
        return
    else:
        return utcparse(as_text(date_str))


def parse_timeout(timeout):
    """Transfer all kinds of timeout format to an integer representing seconds"""
    if not isinstance(timeout, numbers.Integral) and timeout is not None:
        try:
            timeout = int(timeout)
        except ValueError:
            digit, unit = timeout[:-1], (timeout[-1:]).lower()
            unit_second = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
            try:
                timeout = int(digit) * unit_second[unit]
            except (ValueError, KeyError):
                raise TimeoutFormatError('Timeout must be an integer or a string representing an integer, or '
                                         'a string with format: digits + unit, unit can be "d", "h", "m", "s", '
                                         'such as "1h", "23m".')

    return timeout
