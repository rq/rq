#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import optparse

from rq import dummy, Queue, use_connection


def parse_args():
    parser = optparse.OptionParser()
    parser.add_option('-n', '--count', type='int', dest='count', default=1)
    opts, args = parser.parse_args()
    return (opts, args, parser)


def main():
    import sys
    sys.path.insert(0, '.')

    opts, args, parser = parse_args()

    use_connection()

    queues = ('default', 'high', 'low')

    sample_calls = [
        (dummy.do_nothing, [], {}),
        (dummy.sleep, [1], {}),
        (dummy.fib, [8], {}),              # normal result
        (dummy.fib, [24], {}),             # takes pretty long
        (dummy.div_by_zero, [], {}),       # 5 / 0 => div by zero exc
        (dummy.random_failure, [], {}),    # simulate random failure (handy for requeue testing)
    ]

    for i in range(opts.count):
        import random
        f, args, kwargs = random.choice(sample_calls)

        q = Queue(random.choice(queues))
        q.enqueue(f, *args, **kwargs)

        # q = Queue('foo')
        # q.enqueue(do_nothing)
        # q.enqueue(sleep, 3)
        # q = Queue('bar')
        # q.enqueue(yield_stuff)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)
        # q.enqueue(do_nothing)

if __name__ == '__main__':
    main()
