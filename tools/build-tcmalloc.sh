#!/usr/bin/env bash
#
# Script to build libtcmalloc.so in the current WiredTiger git
# repository.
#
# There is no justification for building tcmalloc from scratch, this script serves
# as a break glass "guide" if so needed.
# Rather you should STGRONGLY prefer to grab a pre-built tcmalloc from a suitable host
# for the system you are developing on, and copy that into any relevant git workspaces
# as needed.
#
# This is serves the similar purpose, but is NOT THE SAME as:
#
# test/evergreen/tcmalloc_install_or_build.sh
#
# and the tcmalloc source version referenced here should be updated in sync with that 
# script.

set -euf -o pipefail

die() { echo "$@"; exit 1; }

HERE=$(git rev-parse --show-toplevel) || die "FATAL Run in root of git workspace"
[[ -f ${HERE}/CMakeLists.txt ]] || die "Does not look like a WT workspace - no CMakeLists.txt"
[[ -d ${HERE}/TCMALLOC_LIB ]] && die "TCMALLOC_LIB already present"

# Requires bazel.
which bazel || echo "FATAL Build tool bazel not found in path"

# Use mongo toolchain
[[ -d /opt/mongodbtoolchain/v4/bin ]] || die "FATAL Requires mongodb toolchain."

# This is tcmalloc upstream revision 093ba93 source patched by the SERVER team.
# https://github.com/mongodb-forks/tcmalloc/releases/tag/mongo-SERVER-85737
PATCHED_SRC=mongo-SERVER-85737
PATCHED_TGZ="${PATCHED_SRC}.tar.gz"
PATCHED_TGZ_URL="https://github.com/mongodb-forks/tcmalloc/archive/refs/tags/${PATCHED_TGZ}"
PATCHED_SRC_DIR="tcmalloc-${PATCHED_SRC}"

curl --retry 5 -L $PATCHED_TGZ_URL -sS --max-time 120 --fail --output ${PATCHED_TGZ}
tar zxf ${PATCHED_TGZ}

# Create Bazel BUILD file in the top of the source directory to build
# a shared object.
cat << EOF > ${PATCHED_SRC_DIR}/BUILD
package(default_visibility = ["//visibility:private"])

cc_shared_library(
    name = "libtcmalloc",
    deps = [
     "//tcmalloc:tcmalloc",
    ],
    shared_lib_name = "libtcmalloc.so",
    visibility = ["//visibility:public"],
)
EOF

# Build.
(cd $PATCHED_SRC_DIR
 PATH=/opt/mongodbtoolchain/v4/bin:$PATH bazel build libtcmalloc)

# Install in current repository.
mkdir TCMALLOC_LIB
cp $PATCHED_SRC_DIR/bazel-bin/libtcmalloc.so TCMALLOC_LIB

# Generate script to define "with_tcmalloc" convenience function. 
cat << 'EOF' > TCMALLOC_LIB/with_tcmalloc.sh
with_tcmalloc() {
TOPDIR=\$(git rev-parse --show-toplevel) || { echo "FATAL Not a git repo" && return
SOPATH=\$TOPDIR/TCMALLOC_LIB/libtcmalloc.so 
[[ -f \$SOPATH ]] || echo "FATAL libtcmalloc.so not found"
eval "with_tcmalloc() { LD_PRELOAD=\$SOPATH \\$* ;}"
unset TOPDIR SOPATH
}
EOF

# Generate gdb script for setting up LD_PRELOAD for debugging.
cat <<EOF > TCMALLOC_LIB/libtcmalloc.gdb
set environment LD_PRELOAD $PWD/TCMALLOC_LIB/libtcmalloc.so
EOF

cat <<EOF > TCMALLOC_LIB/readme.txt
WiredTiger utilizes tcmalloc in testing through LD_PRELOAD.

Implicit
========

To load tcmalloc into your current environment run:

export LD_PRELOAD=$PWD/TCMALLOC_LIB/libtcmalloc.so

This will be in effect until you exit the current shell.

NOTE: This will affect ALL binaries run in that environment that
dynamically link to libc.

Explicit
========

Alternatively you can source the script with_tcmalloc.sh in this
directory, like this:

source $PWD/with_tcmalloc.sh

This will define a shell function "with_tcmalloc" that you prefix
commands with to define LD_PRELOAD for that single invocation ONLY.

For example:

 $ with_tcmalloc ./wt -h

Which is equvialent to:

 $ LD_PRELOAD=$PWD/libtcmalloc.so ./wt -h

Debugging
=========

For debugging in gdb you can use $PWD/TCMALLOC_LIB/tcmalloc.gdb

On the command line:

 $ gdb -x $PWD/TCMALLOC_LIB/tcmalloc.gdb wt

Or from within gdb:

 (gdb) source $PWD/TCMALLOC_LIB/tcmalloc.gdb

EOF

# Cache the build product.
tar zcf TCMALLOC_LIB.tgz TCMALLOC_LIB
mkdir -p $CACHED_DIR
mv TCMALLOC_LIB.tgz $CACHED_DIR

exit 0
