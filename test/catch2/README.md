# WiredTiger catch2 framework tests

## Building/running

To build these tests, run CMake with `-DHAVE_UNITTEST=1`, then build as
usual with Ninja.  To run them, go to your build directory and execute
`./test/catch2/catch2-unittests`.

To run tests for a specific tag (a.k.a. subsystem), put the tag in square
brackets, e.g. `[extent_list]`. You can specify multiple tags using commas.

You can also use the `--list-tags` option if you're not sure, or even the
`--help` flag if you're curious and/or lost. There's also further
command-line help at
https://github.com/catchorg/Catch2/blob/devel/docs/command-line.md.

## Contributor's Guide
There are two categories of tests that exist in the catch2 framework. One category focuses on 
performing internal tests towards a WiredTiger module and the other category contains any unit test
that do not fit to a module.

### Modular tests
Any tests to do with an WiredTiger module will be contained in their own module directory. The 
directory would be named after the module, and will have an unit/ and api/ subdirectory. The unit/ 
directory would contain all internal testing to the module, and the api/ subdirectory would contain
all the module API contract testing.

### Miscellaneous tests
Any unit tests that do not belong to a Wirediger module will be contained in the misc_tests
directory.

### Adding a new test
If you want add new tests to an existing subsytem, simply edit the relevant
.cpp file. If you want to test a new subsystem, or a subsystem with no
existing tests, create a new .cpp file and add it to the `SOURCES` list in
`create_test_executable()` (in `CMakeLists.txt`).

A script can be used that automates all of the steps of adding a test under create_test.sh
`Usage: ./create_test.sh [-m module] my_test`
