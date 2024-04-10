#!/usr/bin/env bash
#
# Script to install libtcmalloc.so in the current WiredTiger git
# repository.
#
# This is serves the same purpose, but is NOT THE SAME as:
#
# test/evergreen/tcmalloc_install_or_build.sh
#
# and the tcmalloc source version referenced here should be updated in
# sync with that script.

set -euf -o pipefail

HERE=$(git rev-parse --show-toplevel) || echo "FATAL Run in root of git workspace"
[[ -f ${HERE}/CMakeLists.txt ]] || echo "Does not look like a WT workspace - no CMakeLists.txt"

SRC_ID=mongo-SERVER-85737
CACHED_DIR=${HOME}/.cache/wiredtiger/tcmalloc/${SRC_ID}
CACHED_BIN=${HOME}/.cache/wiredtiger/tcmalloc/${SRC_ID}/TCMALLOC_LIB.tgz
if [[ -r ${CACHED_BIN} ]]; then
    echo "Installing cached copy from $CACHED_BIN"
    tar zxf $CACHED_BIN
    exit
fi

echo "No cached copy on this system. Attempting to build."

# Requires bazel.
which bazel || echo "FATAL Build tool bazel not found in path"

# Use mongo toolchain
[[ -d /opt/mongodbtoolchain/v4/bin ]] || "FATAL Require mongodb toolchain."

# This is tcmalloc upstream revision 093ba93 source patched by the SERVER team.
# https://github.com/mongodb-forks/tcmalloc/releases/tag/mongo-SERVER-85737
PATCHED_SRC=mongo-SERVER-85737
PATCHED_TGZ="${PATCHED_SRC}.tar.gz"
PATCHED_TGZ_URL="https://github.com/mongodb-forks/tcmalloc/archive/refs/tags/${PATCHED_TGZ}"
PATCHED_SRC_DIR="tcmalloc-${PATCHED_SRC}"

curl --retry 5 -L $PATCHED_TGZ_URL -sS --max-time 120 --fail --output ${PATCHED_TGZ}
tar zxf ${PATCHED_TGZ}

# Create this Bazel BUILD file in the top of the source directory to
# build a shared object.
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

(cd $PATCHED_SRC_DIR
 PATH=/opt/mongodbtoolchain/v4/bin:$PATH bazel build libtcmalloc)

# Install in current repository.
mkdir TCMALLOC_LIB
cp $PATCHED_SRC_DIR/bazel-bin/libtcmalloc.so TCMALLOC_LIB

# Cache the build product.
tar zcf TCMALLOC_LIB.tgz TCMALLOC_LIB
mkdir -p $CACHED_DIR
mv TCMALLOC_LIB.tgz $CACHED_DIR

exit 0
