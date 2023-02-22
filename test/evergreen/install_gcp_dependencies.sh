#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

# Install dependencies for the GCP SDK. This method is being used instead of having them installed 
# on system to reduce any versioning problems in the future if the GCP SDK requirements were to
# change. The installation of the libraries were followed from GCP SDK's documentation:
# https://github.com/googleapis/google-cloud-cpp/blob/main/doc/packaging.md
#
install_abseil ()
{
    mkdir abseil-cpp && cd abseil-cpp
    curl -sSL https://github.com/abseil/abseil-cpp/archive/20230125.0.tar.gz -o abseil-cpp.tar.gz
    tar --strip-components=1 -xzf abseil-cpp.tar.gz
    sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' "absl/base/options.h"
    mkdir cmake-out && cd cmake-out
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DABSL_BUILD_TESTING=OFF \
        -DBUILD_SHARED_LIBS=yes ../.
    make -j 4
    cd ..
    sudo /opt/cmake/bin/cmake --build cmake-out --target install
    cd ..
}

install_nlohmann ()
{
    mkdir json && cd json
    curl -sSL https://github.com/nlohmann/json/archive/v3.11.2.tar.gz -o nlohmann-json.tar.gz
    tar --strip-components=1 -xzf nlohmann-json.tar.gz
    mkdir cmake-out && cd cmake-out
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=yes \
        -DBUILD_TESTING=OFF \
        -DJSON_BuildTests=OFF ../.
    make -j 4
    cd ..
    sudo /opt/cmake/bin/cmake --build cmake-out --target install
    cd ..
}

install_crc32c ()
{
    mkdir crc32c && cd crc32c
    curl -sSL https://github.com/google/crc32c/archive/1.1.2.tar.gz -o crc32c.tar.gz
    tar --strip-components=1  -xzf crc32c.tar.gz
    mkdir cmake-out && cd cmake-out
    cmake -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=yes \
        -DCRC32C_BUILD_TESTS=OFF \
        -DCRC32C_BUILD_BENCHMARKS=OFF \
        -DCRC32C_USE_GLOG=OFF ../.
    make -j 4
    cd ..
    sudo /opt/cmake/bin/cmake --build cmake-out --target install
    cd ..
}

install_abseil
install_nlohmann
install_crc32c
