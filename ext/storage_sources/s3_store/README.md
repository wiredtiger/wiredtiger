# WiredTiger's S3 Store Extension

## Building and running 

This is a guide to build WiredTiger with the S3 extension enabled.

##### **First** Build method - meeting AWS dependency through system installed library

This method will find a locally installed version of the AWS S3 and will not require a download stage during build.

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake
cmake -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1 -DENABLE_S3=1 -DENABLE_STRICT=0 -G Ninja ../.
```

* Running with the flag `ENABLE_S3=1` will default to looking for a local installation of S3.

* Optionally the compiler flag `IMPORT_S3_SDK` can also be set to:
    *   `package` for the compiler to search for a local installation of the S3 to use.
    *    This flag should be set alongside the `ENABLE_S3` flag.   

* `ENABLE_STRICT` should be set to 0.
    This is to to turn off strict compiler warnings so it does not pick up different formatting errors of the 3rd party dependenceis.

* Also optionally: `- G Ninja` here to generate a ninja build. 

#### Instructions on installing AWS SDK S3 C++ locally

Follow the guide below to install SDK locally: 
* Note: Set BUILD_ONLY flag to "s3-crt" to install only the necessary dependencies for this extension. 
    *   CMake searches locally for the 's3-crt' package and will not find the 's3' package.

https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup-linux.html


##### **Second** Build method - externally installing AWS S3 during build step

This method will download and compile the AWS S3 every time. 

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake
cmake -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1 -DIMPORT_S3_SDK=external -DENABLE_S3=1 -DENABLE_STRICT=0 -G Ninja ../.
```

* The compiler flag `IMPORT_S3_SDK` should be set to:
    *   `external` to prompt the S3 to be externally installed during the build step.
    *    This should be set alongside the `ENABLE_S3` flag also. 
    *   `ENABLE_S3` defaults to looking for a local version, the `IMPORT_S3_SDK` setting will override the default.

* There are no changes in the other flags mentioned in the first build method. 


## Testing 

#### To run the Tiered python tests for S3:

```bash
# This will run all tiered tests on S3 
env LD_LIBRARY_PATH=.libs python3 ../test/suite/run.py -v 4 tiered
```

#### To run the C unit tests for S3

```bash
# Once WiredTiger has been built with the S3 Extension, run the tests from the build directory 
cd build 

ctest --verbose -R ${TEST NAME}
```

To add any additional unit testing, adding the file into the `ext/storage_sources/s3_store/test` directory will allow the test to be picked up by the `CMakeLists.txt` and be incorporated and run with the above instructions. 