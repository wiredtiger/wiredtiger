# WiredTiger's GCP Extension
## 1. Introduction
This extension allows WiredTiger storage source extensions to read from and write to objects stored in Google Cloud Storage using WiredTiger’s provided internal abstraction for storing data in an object storage service.

## 2. Building and running
This section describes how to build WiredTiger with the GCP extension enabled.

### Requirements
<li> Abseil LTS 20230125
<li> nlohmann_json library 3.11.2
<li> crc32c 1.1.2<p>

### How to install requirements (skip this step if requirements have been met)
Abseil LTS 20230125
```bash
mkdir -p $HOME/Downloads/abseil-cpp && cd $HOME/Downloads/abseil-cpp

curl -ssl https://github.com/abseil/abseil-cpp/archive/20230125.0.tar.gz | tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DABSL_BUILD_TESTING=OFF -DBUILD_SHARED_LIBS=yes ../. && make -j4

cd ..
sudo cmake --build cmake-out --target install
sudo ldconfig
```

nlohmann_json library
```bash
mkdir -p $HOME/Downloads/json && cd $HOME/Downloads/json

curl -ssl https://github.com/nlohmann/json/archive/v3.11.2.tar.gz | tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=yes -DBUILD_TESTING=OFF -DJSON_BuildTests=OFF ../. && make -j4

cd ..
sudo cmake --build cmake-out --target install
sudo ldconfig
```
crc32c
```bash
mkdir -p $HOME/Downloads/crc32c && cd $HOME/Downloads/crc32c

curl -ssl https://github.com/google/crc32c/archive/1.1.2.tar.gz | tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=yes -DCRC32C_BUILD_TESTS=OFF -DCRC32C_BUILD_BENCHMARKS=OFF -DCRC32C_USE_GLOG=OFF ../. && make -j4

cd ..
sudo cmake --build cmake-out --target install
sudo ldconfig
```
### Building
There is current only 1 way to build WiredTiger with GCP extension:
1. Letting CMake manage the GCP SDK dependency as an external project, letting it download, link and build the extension.

There are two CMake flags associated with the GCP extension: `ENABLE_GCP` and `IMPORT_GCP_SDK`.
* `ENABLE_GCP=1` is required to build the GCP extension.
* `IMPORT_GCP_SDK={external}` is used to set the build method.
    *   `external` tells the compiler to search for an existing system installation of the SDK.
    *    This flag should be set alongside the `ENABLE_GCP` flag.
    *    If the `IMPORT_GCP_SDK` flag is not specified, the compiler will assume a system installation of the SDK which will is currently not supported.
### Letting CMake manage the SDK dependency as an external project

This method configures CMake to download, compile, and install the GCP SDK while building the GCP extension.

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake with Ninja
cmake -DENABLE_PYTHON=1 -DHAVE_UNITTEST=1 -DIMPORT_GCP_SDK=external -DENABLE_GCP=1 -G Ninja ../.
ninja
```

* The compiler flag `IMPORT_GCP_SDK` must be set to `external` for this build method.
* `ENABLE_GCP` defaults to looking for a local version, the `IMPORT_GCP_SDK` setting will override that default.

## 3. Development
In order to run this extension after building, the developer must have a GCP credentials file locally with the right permissions. The path to this json file must be stored in an environmental variable called `GOOGLE_APPLICATION_CREDENTIALS`. To store your environmental variable type `export GOOGLE_APPLICATION_CREDENTIALS="path/to/json/"` into your terminal.
## 4. Testing

### To run the tiered python tests for GCP:

```bash
# This will run all the test in test_tiered19.py on the GCP storage source. The following command will run the tests from the build directory that was built earlier.
cd build
env WT_BUILDDIR=$(pwd) python3 ../test/suite/run.py -j 10 -v 4 test_tiered19
```

### To run the C unit tests for GCP:

```bash
# Once WiredTiger has been built with the GCP Extension, run the tests from the build directory
cd build
ext/storage_sources/gcp_store/test/run_GCP_unit_tests
```

To add any additional unit testing, add to the file `test_GCP_connection.cpp`, alternatively if you
wish to add a new test file, add it to the `SOURCES` list in `create_test_executable()`
(in `GCP_store/test/CMakeLists.txt`).

## 5. Evergreen Testing
This section should describe the tasks defined in evergreen.yml for testing the extension code (that a developer would include in patch builds) and how a developer can add additional evergreen tests to the extension.