# WiredTiger's S3 Extension

## Building and running 

This is a guide to build WiredTiger with the S3 extension enabled.

There are two different ways to build WiredTiger with S3 extension:
1. Using a system installation of the S3.
2. Letting CMake manage the S3 dependency as an external project, letting it download and compile each time.

There are two CMake flags associated with the S3 extension: `ENABLE_S3` and `IMPORT_S3_SDK`.
* `ENABLE_S3=1` is required to build the S3 extension.
* `IMPORT_S3_SDK={external,package}` is used to set the build method.
    *   `external` tells the compiler to search for an existing system installation of the S3 SDK.
    *   `package` tells the compiler to download and install the S3 SDK as part of the build.
    *    This flag should be set alongside the `ENABLE_S3` flag. 
    *    If the `IMPORT_S3_SDK` flag is not specified, the compiler will assume a system installation of the SDK.

#### Instructions on installing AWS SDK S3 C++ locally

Follow the [guide to install SDK locally](https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup-linux.html): 
* Note: Set BUILD_ONLY flag to "s3-crt" to install only the necessary dependencies for this extension. 
    *   CMake searches locally for the 's3-crt' package and will not find the 's3' package.


##### **First** build method - meeting AWS dependency through system installed library

This method will find an existing system installation of the AWS S3 and will not require a download stage during build.

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake with Ninja
cmake -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1 -DENABLE_S3=1 -DENABLE_STRICT=0 -G Ninja ../.
ninja
```

* Setting the `IMPORT_S3_SDK` flag is optional for this build method. 
* `ENABLE_S3` will default to looking for a local installation of the S3. 
* `ENABLE_STRICT` should be set to 0.
    This is to to turn off strict compiler warnings so it does not pick up different formatting errors of the 3rd party dependenceis.


##### **Second** build method - externally installing AWS S3 during build step

This method configures CMake to download, compile, and install the AWS S3 every build.

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake with Ninja
cmake -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1 -DIMPORT_S3_SDK=external -DENABLE_S3=1 -DENABLE_STRICT=0 -G Ninja ../.
ninja
```

* The compiler flag `IMPORT_S3_SDK` must be set to `external` for this build method.
* `ENABLE_S3` defaults to looking for a local version, the `IMPORT_S3_SDK` setting will override that default.


## Testing 

#### To run the tiered python tests for S3:

```bash
# This will run all tiered tests on storage sources including S3, run the tests from the build directory
cd build
python3 ../test/suite/run.py -v 4 tiered
```

#### To run the C unit tests for S3

```bash
# Once WiredTiger has been built with the S3 Extension, run the tests from the build directory
cd build 
ctest --verbose -R ${TEST NAME}
```

To add any additional unit testing, adding the file into the `ext/storage_sources/s3_store/test` directory will allow the test to be picked up by the `CMakeLists.txt` and be incorporated and run with the above instructions. 