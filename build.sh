#!/bin/bash

cd build

#cmake -DENABLE_STATIC=1 -DENABLE_LZ4=1 -DENABLE_SNAPPY=1 -DENABLE_ZLIB=1 -DENABLE_ZSTD=1 -DHAVE_DIAGNOSTIC=1 -DHAVE_ATTACH=1 -DENABLE_STRICT=1 -DENABLE_PYTHON=1 -G Ninja ../.

ninja

#ccache --clear
#ccache --clean
#ninja -t clean