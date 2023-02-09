# WiredTiger Memory Model Test

The WiredTiger Memory Model Test performs a series of operations that, depending on the processor,
may generate out-of-order memory accesses that are visible to the code.

It is not a 'pass or fail' type of test. It is a technology demo and testbed for what can occur with
out-of-order memory accesses, and clearly shows in concrete form the differences in behaviour
between the ARM64 and x86_64 memory models.

This test was inspired, in part, by the work at https://preshing.com/20120515/memory-reordering-caught-in-the-act/, 
which is worth reading to understand these tests.

## The Tests

Memory Model Test runs a series of tests. Each test has two threads which are accessing shared variables. 

All shared variables are set to 0 before each test.

There are two broad groups of tests:
- Group 1: 
  - Each thread writes 1 to a shared variable (`x` for thread 1, and `y` for thead 2),
    and then reads from the other shared variable (`y` for thread 1, and `x` for thead 2)
  - Variants of the test are run with no barriers, with one barrier or atomic, or with two barriers or atomics
  - If out-of-order memory accesses occur, then the results of the reads of `x` and `y` can lead to both being 0.
- Group 2:
  - Thread 1 writes 2 to `x` and then 3 to `y`, while thread 2 reads from `y` and then reads from `x`
  - Variants of the test are run with no barriers, with one barrier or atomic, or with two barriers or atomics
  - If out-of-order memory accesses occur, then the results of the reads of `y` being 3, and of `x` being 0.


## Building and running the tests

### Requirements
* ARM64 or x86_64 based CPU
* C++ compiler that supports C++ 20
* (optional) CMake and ninja

### Build

There are two options:

* Use the CMakeLists.txt file with CMake:
  ```
  md build
  cmake -G Ninja ../.
  ninja
  ```
* Use `g++` directly.

  For Evergreen, first ensure that the correct C++ compiler is on the path:
  
  ```
  export "PATH=/opt/mongodbtoolchain/v4/bin:$PATH"
  ```

  On both Mac or Evergreen, compile using g++:
  ```
  g++ -o memory_model_test -O2 memory_model_test.cpp -lpthread -std=c++20
  ```

Some tests use compiler barriers to prevent the compiler re-ordering memory accesses during optimisation.

Note: if you get compile errors related to `#include <semaphore>` or semaphores in general,
then check that you are using both the correct compiler and compiling for C++ 20. 
The test uses the C++ 20 semaphore library as it is supported on both Mac and Ubuntu.

### Running the test

`./memory_model_test` to run the default loop count of 1,000,000 which takes a few tens of seconds.

`./memory_model_test -n <loop count>` to run a custom loop count of `<loop count>`.

### Expected results:

Each test displays if/when out of order operations are possible. 

Some tests should never show out of order operations because of either the correct use of memory barriers/atomics, 
or the processor design. If any of these tests report out of order operations, then that is an error.

Some tests **can** report out of order operations, but that does not mean they **will** report them.
This is due to the randomness of the timing between the two threads, as well as the low incidence of
measurable out-of-order effects on some test/hardware combinations.

All x86_64 and ARM64 processors will likely show some out of order operations for some of the first group of tests.
However, as consequence of the different memory models, only ARM can show out of order operations in the second group.

Current ARM64 results show that group two tests show high out-of-order rates on M1 Pro/Max processors,
but much lower (by several orders of magnitude) out-of-order rates on current Evergreen instances.


# References

- This test was inspired by the work at https://preshing.com/20120515/memory-reordering-caught-in-the-act/
- [Memory barrier use example](https://developer.arm.com/documentation/den0042/a/Memory-Ordering/Memory-barriers/Memory-barrier-use-example) 