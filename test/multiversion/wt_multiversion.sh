#!/bin/bash

PREV_VERSION=4.2

# Build v4.2.
git clone git@github.com:wiredtiger/wiredtiger.git wiredtiger_${PREV_VERSION}/
cd wiredtiger_${PREV_VERSION}/build_posix/
git checkout mongodb-${PREV_VERSION}
bash reconf
../configure --enable-python --enable-diagnostic
make -j 10

# Back to multiversion/ in root repo.
cd ../../
DEVELOP_WORKGEN=../../bench/workgen/runner/example_simple.py
${DEVELOP_WORKGEN}
PREV_WORKGEN=wiredtiger_${PREV_VERSION}/bench/workgen/runner/example_simple.py
${PREV_WORKGEN}
