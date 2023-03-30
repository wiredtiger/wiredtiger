#!/bin/sh
LD_LIBRARY_PATH="$LD_LIBRARY_PATH:./.libs"
PYTHONPATH="$PYTHONPATH:../lang/python:lang/python:../test/suite"
export LD_LIBRARY_PATH
export PYTHONPATH
ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:disable_coredump=0:unmap_shadow_on_exit=1"
ASAN_SYMBOLIZER_PATH=/opt/mongodbtoolchain/v4/bin/llvm-symbolizer
export ASAN_OPTIONS
export ASAN_SYMBOLIZER_PATH
# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libeatmydata.so:$WT_TOPDIR/TCMALLOC_LIB/lib/libtcmalloc.so
PATH=/opt/mongodbtoolchain/v4/bin:$PATH
python3 ../test/suite/run.py --asan -v 10 test_schema09
