#!/bin/bash

# WiredTiger Test Runner with Tracy TLS Fix
# This script sets up the proper environment to handle TLS issues with Tracy profiler

# Set TLS environment variables
export GLIBC_TUNABLES=glibc.rtld.optional_static_tls=2048000
ulimit -s unlimited

# Change to the cmake build directory
cd /home/ubuntu/dev/wiredtiger.worktrees/tracy-profiler/cmake-build-debug

# Set the build directory for tests
export WT_BUILDDIR=$(pwd)

# Fix permissions if running as root/sudo
if [ "$EUID" -eq 0 ]; then
    echo "Running as root, fixing permissions..."
    [ -d WT_TEST ] && chown -R ubuntu:ubuntu WT_TEST
    [ -f WT_TEST/results.txt ] && chown ubuntu:ubuntu WT_TEST/results.txt
fi

# Print environment info
echo "=== TLS Configuration ==="
echo "GLIBC_TUNABLES: $GLIBC_TUNABLES"
echo "WT_BUILDDIR: $WT_BUILDDIR"
echo "Current directory: $(pwd)"
echo "User: $(whoami)"
echo "=========================="

# Run the test with all arguments passed through
exec python3 ../test/suite/run.py "$@"