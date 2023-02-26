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
* clang 6.0.0 or higher

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
          installation of the SDK which will be implemented at a later stage.

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
* `ENABLE_GCP` will error out if the `IMPORT_GCP_SDK` setting is not set.

## 3. Development
In order to run this extension after building, the developer must have a GCP credentials file
locally with the right permissions. The path to this json file must be stored in an environment
variable called `GOOGLE_APPLICATION_CREDENTIALS`. To store the environment variable type
`export GOOGLE_APPLICATION_CREDENTIALS="path/to/json/"` into the terminal.

## 4. Testing

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