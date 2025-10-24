#!/bin/bash
# Source this file to set up TLS environment for Tracy profiler
# Usage: source setup_tracy_env.sh

export GLIBC_TUNABLES=glibc.rtld.optional_static_tls=2048000
ulimit -s unlimited

echo "Tracy TLS environment configured:"
echo "GLIBC_TUNABLES=$GLIBC_TUNABLES"