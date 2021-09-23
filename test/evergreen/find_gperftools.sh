#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

find_gperftools()
{
    # curl https://s3.amazonaws.com/boxes.10gen.com//Users/jiechenbo_build/test.tar.gz -o test.tar.gz

    echo "-- MAKE CMAKE --"
    CMAKE_INSTALL_DIR=$(readlink -f cmake-install)
    curl --retry 5 -L https://github.com/gperftools/gperftools/releases/download/gperftools-2.9.1/gperftools-2.9.1.tar.gz -sS --max-time 120 --fail --output tcmalloc.tar.gz
    tar xzf tcmalloc.tar.gz
    cd gperftools-2.9.1
    sh ./configure --prefix=$(pwd)/TCMALLOC_LIB 
    make install

    tar czf test.tar.gz $(pwd)/TCMALLOC_LIB 
    s3put --grant public-read --bucket boxes.10gen.com  --callback 5 --prefix /Users/jiechenbo_build/test.tar.gz

    export CPPFLAGS='-I$(pwd)/TCMALLOC_LIB/include/'
    export LDFLAGS='-L$(pwd)/TCMALLOC_LIB/lib/'
    echo "-- DONE MAKING GPERFTOOLS --"
}

find_gperftools
