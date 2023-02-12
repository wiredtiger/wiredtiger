# WiredTiger's GCP Extension

## How to Build and Run

This is a guide to build WiredTiger with the GCP extension enabled.

There is current only 1 way to build WiredTiger with GCP extension:
1. Letting CMake manage the GCP SDK dependency as an external project, letting it download, link and build the extension.

There are two CMake flags associated with the GCP extension: `ENABLE_GCP` and `IMPORT_GCP_SDK`.
* `ENABLE_GCP=1` is required to build the GCP extension.
* `IMPORT_GCP_SDK={external}` is used to set the build method.
    *   `external` tells the compiler to search for an existing system installation of the SDK.
    *    This flag should be set alongside the `ENABLE_GCP` flag.
    *    If the `IMPORT_GCP_SDK` flag is not specified, the compiler will assume a system installation of the SDK which will be implemented at a later stage.

### First build method - letting CMake manage the SDK dependency as an external project

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


## Testing

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
