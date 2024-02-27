#!/bin/sh
# This script clones Metrix++ and checks out a specific commit
git clone https://github.com/metrixplusplus/metrixplusplus metrixplusplus || exit

cd metrixplusplus || exit

# Check out the version from 18th Dec 2022, which is the latest commit as of 28th Feb 2024.
git checkout 78dc5380de9aaa3d615f8be6c84e90cb2ae0d90b || exit

cd ..
