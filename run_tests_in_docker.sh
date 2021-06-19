#!/bin/bash

docker build -f tests/Dockerfile . -t rqtest && docker run -it --rm rqtest
