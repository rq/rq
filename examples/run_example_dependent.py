# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import os
import time

from rq import Connection, Queue

from rnd_delay import do_it, print_finish, last_one


def main():
    q = Queue()
    q2 = Queue("Queue2")
    q3 = Queue("Queue3")
    j = q.enqueue(do_it, 1)
    print(j)
    j2 = q2.enqueue(do_it, 1, depends_on=j)
    print(j2)
    j3 = q3.enqueue(do_it, 1, depends_on=j2)
    print(j3)
    j4 = q3.enqueue(do_it, 1, depends_on=j3)
    print(j4)
    print('Done enqueing')

if __name__ == '__main__':
    # Tell RQ what Redis connection to use
    with Connection():
        main()
