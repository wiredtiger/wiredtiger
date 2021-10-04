#!/bin/sh
# Currently evergreen machine doesn't contain gperftools binaries. The aim of this script is to
# create a caching mechanism for fetching gperftools version 2.9.1. First we check if the binaries
# for the machine are present in 10gen S3 bucket. If not, we download the sources, build the
# binaries and upload the binaries to 10gen S3 Bucket.
#
# Note: The specific version number of gperftools 2.9.1 follows what mongodb currently uses. 
#
set +o verbose
set +o errexit

# This script requires the aws key, aws secret, build variant, and boolean describing if we are
# compiling for a cmake build. Example usage: ./find_gerftools.sh aws_key aws_secret ubuntu2004 true
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters."
    exit 1
fi

# Export the aws key and aws secret which are necessary to use aws cli.
export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2
build_variant=$3
is_cmake_build=$4

# Track the original directory, so we can build in the directory that calls this script.
dir=$(pwd)

# Fetch the gperftools library through either building the sources of gerftools 2.9.1 or directly
# downloading the binaries from the S3 bucket.
fetch_gperftools()
    echo "-- FETCH GPERFTOOLS --"
    aws s3 ls "s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz"
    # Check if the output of searching for the gperftools binaries was successful or not. If
    # successful download the binaries from the S3 bucket. Otherwise download and build the
    # gperftools binaries.
    if [[ $? -eq 0 ]]; then
        aws s3 cp s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz tcmalloc_${build_variant}.tgz
        tar xzf tcmalloc_${build_variant}.tgz --directory ${dir}
    else
        echo "-- MAKE GPERFTOOLS --"
        curl --retry 5
            -L https://github.com/gperftools/gperftools/releases/download/gperftools-2.9.1/gperftools-2.9.1.tar.gz-sS
            --max-time 120 gperftools_${build_variant}.tgz
        tar xzf gperftools_${build_variant}.tgz
        cd gperftools-2.9.1
        sh ./configure --prefix=${dir}/TCMALLOC_LIB
        make install -j $(grep -c ^processor /proc/cpuinfo)
        cd ..
        tar czf tcmalloc_${build_variant}.tgz -C ${dir} TCMALLOC_LIB
        aws s3 cp tcmalloc_${build_variant}.tgz s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz
    fi
    echo "-- DONE GPERFTOOLS --"
}

fetch_gperftools

# After fetching the gperftools, link the libraries only if it isn't a cmake build. Cmake build
# system manages this internally.
if [ "$is_cmake_build" = false ]; then
    export CPPFLAGS="$CPPFLAGS -I${dir}/TCMALLOC_LIB/include"
    export LDFLAGS="$LDFLAGS -L${dir}/TCMALLOC_LIB/lib"
    export LD_LIBRARY_PATH="$(git rev-parse --show-toplevel)/TCMALLOC_LIB/lib:$LD_LIBRARY_PATH"
fi
set -o errexit
set -o verbose
