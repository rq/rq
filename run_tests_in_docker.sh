#!/bin/bash

docker build . -t rqtest && docker run -it --rm rqtest
