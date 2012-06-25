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
    parser.add_argument('-i', '--interval', default=60, type=int
        , help="How often the scheduler checks for new jobs to add to the \
            queue (in seconds).")
    args = parser.parse_args()
    connection = Redis(args.host, args.port, args.db, args.password)
    scheduler = Scheduler(connection=connection, interval=args.interval)
    scheduler.run()

if __name__ == '__main__':
    main()
