#!/bin/bash

# Set up environment to handle TLS issues with Tracy profiler
export GLIBC_TUNABLES=glibc.rtld.optional_static_tls=2048000
ulimit -s unlimited

# Change to build directory
cd /home/ubuntu/dev/wiredtiger.worktrees/tracy-profiler/cmake-build-debug

# Run with sudo but preserve our environment
if [ "$EUID" -eq 0 ]; then
    # Already running as root
    env WT_BUILDDIR=$(pwd) python3 ../test/suite/run.py "$@"
else
    # Need sudo, preserve environment
    sudo -E env WT_BUILDDIR=$(pwd) python3 ../test/suite/run.py "$@"
fi