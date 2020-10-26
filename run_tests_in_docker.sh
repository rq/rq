#!/bin/bash

docker build . -t rqtest && docker run --rm rqtest
