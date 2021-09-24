#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail
if [ "$#" -ne 2 ]; then
    echo $#
    echo "Illegal number of parameters."
    exit 1
fi

aws_key=$1
aws_secret=$2
echo ${aws_key}
find_gperftools()
{
    curl -L https://s3.amazonaws.com/build_external/jiechenbo_build/test.tar.gz -o test.tar.gz

    echo "-- MAKE GPERFTOOLS --"
    curl --retry 5 -L https://github.com/gperftools/gperftools/releases/download/gperftools-2.9.1/gperftools-2.9.1.tar.gz -sS --max-time 120 --fail --output tcmalloc.tar.gz
    tar xzf tcmalloc.tar.gz
    cd gperftools-2.9.1
    sh ./configure --prefix=$(pwd)/TCMALLOC_LIB 
    make install -j $(grep -c ^processor /proc/cpuinfo)

    tar czf test.tar.gz $(pwd)/TCMALLOC_LIB 
    s3put --access_key ${aws_key} --secret_key ${aws_secret} --grant public-read --bucket build_external --callback 5 --prefix /jiechenbo_build/test.tar.gz path test.tar.gz

    export CPPFLAGS='-I$(pwd)/TCMALLOC_LIB/include/'
    export LDFLAGS='-L$(pwd)/TCMALLOC_LIB/lib/'
    echo "-- DONE MAKING GPERFTOOLS --"
}

find_gperftools
