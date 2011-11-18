"""
Some dummy tasks that are well-suited for generating load for testing purposes.
"""
import time

def do_nothing():
    pass

def sleep(secs):
    time.sleep(secs)

def endless_loop():
    x = 7
    while True:
        x *= 28
        if x % 3 == 0:
            x //= 21
        if x == 0:
            x = 82

def div_by_zero():
    1/0

def yield_stuff():
    yield 7
    yield 'foo'
    yield (3.14, 2.18)
    yield yield_stuff()
