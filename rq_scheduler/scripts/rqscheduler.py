#!/usr/bin/env python

import argparse

from redis import Redis
from rq_scheduler.scheduler import Scheduler


def main():
    parser = argparse.ArgumentParser(description='Runs RQ scheduler')
    parser.add_argument('-H', '--host', default='localhost', help="Redis host")
    parser.add_argument('-p', '--port', default=6379, type=int, help="Redis port number")
    parser.add_argument('-d', '--db', default=0, type=int, help="Redis database")
    parser.add_argument('-P', '--password', default=None, help="Redis password")
    args = parser.parse_args()
    connection = Redis(args.host, args.port, args.db, args.password)
    scheduler = Scheduler(connection=connection)
    scheduler.run()

if __name__ == '__main__':
    main()
