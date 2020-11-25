FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y redis-server python3-pip stunnel

RUN openssl genrsa -out /etc/stunnel/key.pem 4096  \
    && openssl req -new -x509 -key /etc/stunnel/key.pem -out /etc/stunnel/cert.pem -days 1826 -subj "/emailAddress=test@getresq.com/CN=rq.com/O=RQ/OU=Eng/C=CA/ST=Test/L=Test" \
    && cat /etc/stunnel/key.pem /etc/stunnel/cert.pem > /etc/stunnel/private.pem \
    && chmod 640 /etc/stunnel/key.pem /etc/stunnel/cert.pem /etc/stunnel/private.pem \
    && echo 'redis\t1234/tcp' >> /etc/services \
    && echo 'cert=/etc/stunnel/private.pem' >> /etc/stunnel/stunnel.conf \
    && echo 'fips=no' >> /etc/stunnel/stunnel.conf \
    && echo 'foreground=yes' >> /etc/stunnel/stunnel.conf \
    && echo 'sslVersion=all' >> /etc/stunnel/stunnel.conf \
    && echo 'socket=l:TCP_NODELAY=1' >> /etc/stunnel/stunnel.conf \
    && echo 'socket=r:TCP_NODELAY=1' >> /etc/stunnel/stunnel.conf \
    && echo 'pid=/var/run/stunnel.pid' >> /etc/stunnel/stunnel.conf \
    && echo 'debug=0' >> /etc/stunnel/stunnel.conf \
    && echo 'output=/etc/stunnel/stunnel.log' >> /etc/stunnel/stunnel.conf \
    && echo '[redis]' >> /etc/stunnel/stunnel.conf >> /etc/stunnel/stunnel.conf \
    && echo 'accept = 0.0.0.0:1234' >> /etc/stunnel/stunnel.conf  \
    && echo 'connect = 127.0.0.1:6379' >> /etc/stunnel/stunnel.conf

COPY . /tmp/rq
WORKDIR /tmp/rq
RUN pip3 install -r /tmp/rq/requirements.txt -r /tmp/rq/dev-requirements.txt
RUN python3 /tmp/rq/setup.py build && python3 /tmp/rq/setup.py install

CMD stunnel & redis-server & RUN_SLOW_TESTS_TOO=1 pytest /tmp/rq/tests/ --durations=5 -v --log-cli-level 10
