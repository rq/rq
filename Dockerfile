FROM python:3.8

WORKDIR /root

COPY . /tmp/rq

RUN pip install /tmp/rq; \
    rm -r /tmp/rq

ENTRYPOINT ["rq", "worker"]
