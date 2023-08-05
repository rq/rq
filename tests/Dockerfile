FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# See: https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa for supported versions of Python.
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
    build-essential \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    wget \
    software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update

RUN apt-get install -y \
    redis-server \
    python3-pip \
    stunnel \
    python3.7 \
    python3.7-dev \
    python3.7-distutils \
    python3.8 \
    python3.8-dev \
    python3.9 \
    python3.9-dev \
    python3.10 \
    python3.10-dev \
    python3.11 \
    python3.11-dev

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY tests/ssl_config/private.pem tests/ssl_config/stunnel.conf /etc/stunnel/

COPY . /tmp/rq
WORKDIR /tmp/rq

RUN set -e && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir hatch tox

CMD stunnel \
    & redis-server \
    & RUN_SLOW_TESTS_TOO=1 RUN_SSL_TESTS=1 hatch run tox
