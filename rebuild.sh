#!/bin/bash

cd .
export CC=clang
cd `git rev-parse --show-toplevel`
rm -rf build
mkdir build
cd build
cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCODE_STATIC_CHECK=ON -G Ninja .. && ninja "$@"
cd .

