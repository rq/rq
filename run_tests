#!/bin/sh
check_redis_running() {
    redis-cli echo "just checking" > /dev/null
    return $?
}

if which -s rg; then
    safe_rg=rg
else
    # Fall back for systems that don't have rg installed
    safe_rg=cat
fi

export ONLY_RUN_FAST_TESTS=1
if [ "$1" == '-f' ]; then  # Poor man's argparse
    unset ONLY_RUN_FAST_TESTS
fi

if check_redis_running; then
    /usr/bin/env python -m unittest discover -v -s tests $@ 2>&1 | egrep -v '^test_' | $safe_rg
else
    echo "Redis not running." >&2
    exit 2
fi
