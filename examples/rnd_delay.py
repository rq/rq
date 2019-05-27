# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from random import randint
from time import sleep

def do_it(sleep_time):
    #sleep_time = randint(1, 10)
    print("HERE I GO... {0}".format(sleep_time))
    sleep(sleep_time)
    print("DONE!")
    return sleep_time

def last_one():
    print("I KNOW I'M THE LAST!")
    sleep(20)
    print("YOU WAITED FOR ME!")

def print_finish():
    print("WE SHOULD WAIT UNTIL EVERYBODY HAS FINISHED")
