# WiredTiger Testing Overview

## Introduction
The reliability and robustness of WiredTiger is achieved in part by thorough and careful testing.

## Testing types
There are many testing areas covered by WiredTiger's test infrasture. Some of the high level areas are:

### Compatibility testing

### Correctness testing
- suite (Python / Unit tests)
- csuite

### Performance testing
- wtperf
- workgen

### Stress testing
- format

### Runtime correctness checking
- ASAN, MSAN, UBSAN, Valgrind 

### Static analysis
- Coverity

## Test frameworks

### CSuite

### CPPSuite

### Format

### Python
Whitebox testing. Focus on specific features but others are used alongside.

### WTPerf

### Workgen

### Evergreen
See [GitHub repository](https://github.com/evergreen-ci/evergreen/wiki).

## Coverage

### Tests
The [test coverage documentation](../output.md) is generated from tags that are present in each test file (main.c and *.py)

#### Tagging scheme
Each test file shall contain a tag using the following scheme:

```
[TEST_TAGS]
<COMPONENT>:<TESTING_TYPE>:<TESTING_AREA>
[END_TAGS]
```

One test file can have multiple tags:

```
[TEST_TAGS]
backup:correctness:full_backup
checkpoints:correctness:checkpoint_data
caching_eviction:correctness:written_data
[END_TAGS]
```

If a test file shall be ignored, the following tag can be used:

```
[TEST_TAGS]
ignored_file
[END_TAGS]
```

### Code
WiredTiger code coverage is measured by gcov, >75% of lines are currently covered by the current testing framework.

### Cyclomatic Complexity