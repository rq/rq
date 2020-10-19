FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y redis-server python3-pip

RUN pip3 install redis>=3.5.0 click>=5.0.0 mock pytest sentry_sdk

COPY . /tmp/rq
WORKDIR /tmp/rq
RUN python3 /tmp/rq/setup.py build && python3 /tmp/rq/setup.py install

CMD redis-server& RUN_SLOW_TESTS_TOO=1 pytest /tmp/rq/tests/ --durations=5 -v --log-cli-level 10
