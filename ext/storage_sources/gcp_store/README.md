# WiredTiger's GCP Extension
## 1. Introduction
This extension allows WiredTiger storage source extensions to read from and write to objects stored
in Google Cloud Storage using WiredTiger’s provided internal abstraction for storing data in an
object storage service.

## 2. Building and running
This section describes how to build WiredTiger with the GCP extension enabled.

### Requirements
* CMake 3.11 or higher
* G++ 8.4 or higher
* Abseil LTS 20230125
* nlohmann_json library 3.11.2
* crc32c 1.1.2

### How to install requirements (skip this step if requirements have been met)
If the CMake version is not 3.11 or higher update CMake to 3.11 using the following.
```bash
sudo apt remove cmake
wget https://cmake.org/files/v3.11/cmake-3.11.0.tar.gz
tar xf cmake-3.11.0.tar.gz

cd cmake-3.11.0

./configure
make -j $(nproc)
sudo make install
```
Check that CMake has been updated using the following command `cmake --version`.

If cmake is not in `/usr/bin/` create a symbolic link using the following.

```bash
sudo ln -s /usr/local/bin/cmake /usr/bin/cmake
```

If the G++ version is not 8.4 or higher update G++ to 8.4 using the following.

```bash
sudo apt-get install gcc-8 g++-8
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 20
  --slave /usr/bin/g++ g++ /usr/bin/g++-8
```
Check that G++ has been updated using the following command `g++ --version`.

### Check Abseil exists locally
```bash
sudo find /usr/ -iname absl
# if /usr/local/lib/cmake/absl or /usr/local/include/absl is not found absl doesn't exist!

# Instructions to download Abseil if Abseil doesn't exist.
mkdir -p $HOME/Downloads/abseil-cpp && cd $HOME/Downloads/abseil-cpp

curl -sSL https://github.com/abseil/abseil-cpp/archive/20230125.0.tar.gz | \
  tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DABSL_BUILD_TESTING=OFF -DBUILD_SHARED_LIBS=yes ../.
  && make -j $(nproc)

cd ..
sudo cmake --build cmake-out --target install
```

### Check the nlohmann_json library exists locally
```bash
sudo find /usr/ -iname nlohmann
# if /usr/local/include/nlohmann is not found nlohmann_json ibrary doesn't exist!

# Instructions to download nlohmann_json library if nlohmann_json library doesn't exist.
mkdir -p $HOME/Downloads/json && cd $HOME/Downloads/json

curl -sSL https://github.com/nlohmann/json/archive/v3.11.2.tar.gz | \
  tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=yes -DBUILD_TESTING=OFF
  -DJSON_BuildTests=OFF ../. && make -j $(nproc)

cd ..
sudo cmake --build cmake-out --target install
```

### Check if crc32c exists locally
```bash
sudo find /usr/ -iname crc32c
# if /usr/local/lib/cmake/Crc32c or /usr/local/include/crc32c is not found crc32c doesn't exist!

# Instructions to download crc32c if crc32c doesn't exist.
mkdir -p $HOME/Downloads/crc32c && cd $HOME/Downloads/crc32c

curl -sSL https://github.com/google/crc32c/archive/1.1.2.tar.gz | \
  tar -xzf - --strip-components=1

mkdir cmake-out && cd cmake-out

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=yes -DCRC32C_BUILD_TESTS=OFF
  -DCRC32C_BUILD_BENCHMARKS=OFF -DCRC32C_USE_GLOG=OFF ../. && make -j $(nproc)

cd ..
sudo cmake --build cmake-out --target install
```
### Building
There is currently only 1 way to build WiredTiger with GCP extension:
1. Letting CMake manage the GCP SDK dependency as an external project, letting it download, link
  and build the extension.

There are two CMake flags associated with the GCP extension: `ENABLE_GCP` and `IMPORT_GCP_SDK`.
* `ENABLE_GCP=1` is required to build the GCP extension.
* `IMPORT_GCP_SDK={external}` is used to set the build method.
    *   `external` tells the compiler to search for an existing system installation of the SDK.
    *    This flag should be set alongside the `ENABLE_GCP` flag.
    *    If the `IMPORT_GCP_SDK` flag is not specified, the compiler will assume a system
          installation of the SDK which will is currently not supported.
### Letting CMake manage the SDK dependency as an external project

This method configures CMake to download, compile, and install the GCP SDK while building
the GCP extension.

```bash
# Create a new directory to run the build from
$ mkdir build && cd build

# Configure and run cmake with Ninja
cmake -DENABLE_PYTHON=1 -DHAVE_UNITTEST=1 -DIMPORT_GCP_SDK=external -DENABLE_GCP=1 -G Ninja ../.
ninja
```

* The compiler flag `IMPORT_GCP_SDK` must be set to `external` for this build method.
* `ENABLE_GCP` defaults to looking for a local version, the `IMPORT_GCP_SDK` setting will
    override that default.

## 3. Development
In order to run this extension after building, the developer must have a GCP credentials file
locally with the right permissions. The path to this json file must be stored in an environment
variable called `GOOGLE_APPLICATION_CREDENTIALS`. To store the environment variable type
`export GOOGLE_APPLICATION_CREDENTIALS="path/to/json/"` into the terminal.

## 4. Testing

Before running the tests set the LD_LIBRARY_PATH to tell the loader where to look for the
dynamic shared libraries that we made earlier.

```bash
export LD_LIBRARY_PATH=$(pwd)/gcp-sdk-cpp/install/lib:/usr/local/lib/:$LD_LIBRARY_PATH
```
### To run the tiered python tests for GCP:

```bash
# This will run all the tests in test_tiered19.py on the GCP storage source. The following command
# will run the tests from the build directory that was made earlier.
cd build
env WT_BUILDDIR=$(pwd) python3 ../test/suite/run.py -j 10 -v 4 test_tiered19
```

### To run the C unit tests for GCP:

```bash
# Once WiredTiger has been built with the GCP Extension, run the tests from the build directory
cd build
ext/storage_sources/gcp_store/test/run_gcp_unit_tests
```

To add any additional unit testing, add to the file `test_GCP_connection.cpp`, alternatively if the
developer wishes to add a new test file, add it to the `SOURCES` list in `create_test_executable()`
(in `GCP_store/test/CMakeLists.txt`).

## 5. Evergreen Testing

Currently the Evergreen testing runs both `test_tiered19.py` and the unit tests in
`test_gcp_connection.cpp`. Should a developer wish to additional tests to the extension, they would
first have to write the tests before adding it as a task to the evergreen.yml file.

Additionally, Evergreen has hidden the private key and private key id for GCP and these are stored
within the Evergreen system. Due to GCP requiring a json authentication file to authenticate the
connection, a temporary file of the json authentication file is required to be subsitituted with the
private key and private key id hidden earlier. Evergreen also has a script to install all the
dependencies that GCP requires.