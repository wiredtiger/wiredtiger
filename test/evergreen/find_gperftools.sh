#!/bin/sh
set +o verbose
set +o errexit
if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters."
    exit 1
fi

dir=$(pwd)
aws_key=$1
aws_secret=$2
build_variant=$3
is_cmake_build=$4

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2
find_gperftools()
{
    echo tcmalloc_${build_variant}.tgz
    echo "-- FETCH GPERFTOOLS --"
    aws s3 ls "s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz"
    if [[ $? -ne 0 ]]; then
        echo "-- MAKE GPERFTOOLS --"
        curl --retry 5 -L https://github.com/gperftools/gperftools/releases/download/gperftools-2.9.1/gperftools-2.9.1.tar.gz -sS --max-time 120 --fail --output gperftools_${build_variant}.tgz
        tar xzf gperftools_${build_variant}.tgz
        cd gperftools-2.9.1
        sh ./configure --prefix=${dir}/TCMALLOC_LIB
        make install -j $(grep -c ^processor /proc/cpuinfo)

        tar czf tcmalloc_${build_variant}.tgz -C ${dir} TCMALLOC_LIB
        aws s3 cp tcmalloc_${build_variant}.tgz s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz
    else
        aws s3 cp s3://build_external/jiechenbo_build/tcmalloc_${build_variant}.tgz tcmalloc_${build_variant}.tgz
        tar xzf tcmalloc_${build_variant}.tgz --directory ${dir}
    fi  

    if [ "$is_cmake_build" = false ]; then
        export CPPFLAGS="$CPPFLAGS -I${dir}/TCMALLOC_LIB/include"
        export LDFLAGS="$LDFLAGS -L${dir}/TCMALLOC_LIB/lib"
        export LD_LIBRARY_PATH="${dir}/TCMALLOC_LIB/lib:$LD_LIBRARY_PATH"
    fi
    echo "-- DONE GPERFTOOLS --"
}

find_gperftools

cd ${dir}
set -o errexit
set -o verbose