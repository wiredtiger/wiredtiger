# S3 Store Extension

## Building and running

```bash
# Create a new directory to run your build from
$ mkdir build && cd build

# Configure and run cmake
cmake -DENABLE_PYTHON=1 -DHAVE_DIAGNOSTIC=1 -DENABLE_S3=1 -DENABLE_STRICT=0 -G Ninja ../.
```

* Running with the tag '-DENABLE_S3=1' will default to looking for a local installation of S3.
       
* Optionally the compiler flag 'IMPORT_S3_SDK' can be set to either:
    -  'external' prompts the S3 to be externally installed during the build step.
    -  'package' for the compiler to search for a local installation of the S3 to use.  

* 'DENABLE_STRICT' is set to 0 to turn off strict compiler warnings so it does not pick up different formatting errors of the 3rd party
    dependenceis.

* Also optionally: '- G Ninja' here to generate a ninja build. _


### Installing AWS SDK S3 C++ locally

Follow the guide below to install SDK locally: 
https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/setup-linux.html


## Testing 

Testing should be done after building WiredTiger with the S3 Extension 

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

To add any additional unit testing, adding the file into the ext/storage_sources/s3_store/test directory will allow the test to be picked up by the CMakeLists.txt and be incorporated and run with the above instructions. 