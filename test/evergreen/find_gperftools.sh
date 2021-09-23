#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

find_gperftools()
{
    if [ ! -z "$CMAKE" ]; then
        return 0
        
    if [ -z "$CMAKE" -o -z "$( $CMAKE --version 2>/dev/null )" ]; then
        # Some images have no cmake yet, or a broken cmake (see: BUILD-8570)
        echo "-- MAKE CMAKE --"
        CMAKE_INSTALL_DIR=$(readlink -f cmake-install)
        curl --retry 5 https://cmake.org/files/v3.11/cmake-3.11.0.tar.gz -sS --max-time 120 --fail --output cmake.tar.gz
        tar xzf cmake.tar.gz
        cd cmake-3.11.0
        ./bootstrap --prefix="${CMAKE_INSTALL_DIR}"
        make -j8
        make install
        cd ..
        CMAKE="${CMAKE_INSTALL_DIR}/bin/cmake"
        CTEST="${CMAKE_INSTALL_DIR}/bin/ctest"
        echo "-- DONE MAKING CMAKE --"
    fi

    echo "=========================================================="
    echo "CMake and CTest environment variables, paths and versions:"
    echo "CMAKE: ${CMAKE}"
    echo "CTEST: ${CTEST}"
    command -v ${CMAKE}
    command -v ${CTEST}
    ${CMAKE} --version
    ${CTEST} --version
    echo "=========================================================="
}

find_gperftools
