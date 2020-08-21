#!/bin/sh
# Run tox tests compatible with the current python version.
# Only run unit tests, no static check such as flake8.
#
# Why use seperated script:
#   It is easy to specify matrix testenvs using "factors" concept of tox. But in command line you can not run subset of
#   testenvs by factor matching.
#   There are some tox plugins to do factor maching in command line, but a script is easy and simple.

PYNUMBER=$(python -c 'import sys; print("".join((str(x) for x in sys.version_info[0:2])))')
PYNAME="py${PYNUMBER}"
ENVS=$(tox -l|grep -E "^${PYNAME}-.*")

if [ -z "${ENVS}" ]
then
    echo "Current python verion not in tox configuration: ${PYNAME}" >&2
    exit 1
fi

for ENV in ${ENVS}
do
    echo "Tests starting for env ${ENV}"
    if ! tox -e "${ENV}"
    then
        echo "Failed tox test env: ${ENV}" >&2
        exit 1
    fi
done
