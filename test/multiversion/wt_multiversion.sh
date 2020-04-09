#!/bin/bash

LAST_STABLE=4.2
LAST_STABLE_DIR=wiredtiger_${LAST_STABLE}/
LAST_STABLE_BRANCH=mongodb-${LAST_STABLE}

# Exporting to satisfy ShellCheck.
export LAST_STABLE_BRANCH

# FIXME-WT-5986: Temporarily point this at a local branch for testing.
# Remove this once workgen args are backported to v4.2.
TMP_BRANCH=wt-5989-workgen-args-4_2

function setup_last_stable {
    git clone git@github.com:wiredtiger/wiredtiger.git ${LAST_STABLE_DIR}
    cd ${LAST_STABLE_DIR}/build_posix/ || exit
    git checkout $TMP_BRANCH
    bash reconf
    ../configure --enable-python --enable-diagnostic
    make -j 10
    # Back to multiversion/ in "latest" repo.
    cd ../../ || exit
}

# Clone and build v4.2 if it doesn't already exist.
if [ ! -d $LAST_STABLE_DIR ]; then
    setup_last_stable
fi

LATEST_WORKGEN=../../bench/workgen/runner/multiversion.py
LAST_STABLE_WORKGEN=${LAST_STABLE_DIR}/bench/workgen/runner/multiversion.py

# Copy the workload into the v4.2 tree.
cp $LATEST_WORKGEN $LAST_STABLE_WORKGEN

$LATEST_WORKGEN --release 4.4
$LATEST_WORKGEN -K --release 4.4
$LAST_STABLE_WORKGEN -K --release 4.2
$LATEST_WORKGEN -K --release 4.4
