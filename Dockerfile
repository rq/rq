FROM ubuntu:latest

RUN apt-get update \
    && apt-get install -y \
    redis-server \
    python3-pip \
    stunnel

COPY tests/ssl_config/private.pem tests/ssl_config/stunnel.conf /etc/stunnel/

COPY . /tmp/rq
WORKDIR /tmp/rq
RUN pip3 install -r /tmp/rq/requirements.txt -r /tmp/rq/dev-requirements.txt \
    && python3 /tmp/rq/setup.py build \
    && python3 /tmp/rq/setup.py install

CMD stunnel \
    & redis-server \
    & RUN_SLOW_TESTS_TOO=1 RUN_SSL_TESTS=1 pytest /tmp/rq/tests/ --durations=5 -v --log-cli-level 10
