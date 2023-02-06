#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

install_abseil ()
{
    mkdir -p Downloads/abseil-cpp && cd Downloads/abseil-cpp
    curl -sSL https://github.com/abseil/abseil-cpp/archive/20230125.0.tar.gz | \
        tar -xzf - --strip-components=1 && \
        sed -i 's/^#define ABSL_OPTION_USE_\(.*\) 2/#define ABSL_OPTION_USE_\1 0/' "absl/base/options.h" && \
        . ../../../../../test/evergreen/find_cmake.sh
        mkdir cmake-out && cd cmake-out
            /opt/cmake/bin/cmake \
            -DCMAKE_BUILD_TYPE=Release \
            -DABSL_BUILD_TESTING=OFF \
            -DBUILD_SHARED_LIBS=yes ../. && \
        make -j4
        cd ..
        sudo /opt/cmake/bin/cmake --build cmake-out --target install
}

install_nlohmann ()
{
    mkdir json && cd json
    curl -sSL https://github.com/nlohmann/json/archive/v3.11.2.tar.gz | \
        tar -xzf - --strip-components=1 && \
        . ../../../../../test/evergreen/find_cmake.sh
        mkdir cmake-out && cd cmake-out
        /opt/cmake/bin/cmake \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=yes \
            -DBUILD_TESTING=OFF \
            -DJSON_BuildTests=OFF ../. && \
        make -j4
        cd ..
        sudo /opt/cmake/bin/cmake --build cmake-out --target install
}

install_crc32c ()
{
    mkdir crc32c && cd crc32c
    curl -sSL https://github.com/google/crc32c/archive/1.1.2.tar.gz | \
        tar -xzf - --strip-components=1 && \
        . ../../../../../test/evergreen/find_cmake.sh
        mkdir cmake-out && cd cmake-out
        /opt/cmake/bin/cmake \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=yes \
            -DCRC32C_BUILD_TESTS=OFF \
            -DCRC32C_BUILD_BENCHMARKS=OFF \
            -DCRC32C_USE_GLOG=OFF ../. && \
        make -j4
        cd ..
        sudo /opt/cmake/bin/cmake --build cmake-out --target install
}

install_abseil
install_nlohmann
install_crc32c