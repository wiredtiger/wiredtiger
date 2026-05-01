# csuite_style_example_test — Template demonstrating how to write a C++ test using cppsuite utilities without the framework

**File:** `test/cppsuite/tests/csuite_style_example_test.cpp`
**Storage mode:** General
**Components under test:** `connection_manager`, `thread_manager`, `logger`, `random_generator`, cursor insert, cursor search

## Overview

This file is an example test (not a framework-class-based test) that shows how to use cppsuite utilities — connection manager, thread manager, logger, and random generator — in the style of a traditional C-suite test. It does not subclass `test`, does not use configuration files, and does not integrate with the workload manager or operation tracker. It opens a connection, creates a single collection, inserts one record, reads it back, then spawns concurrent insert and read threads for 5 seconds. It serves as a template and tutorial for developers who prefer a simpler, more direct testing style.

## Configuration

This test has **no configuration file**. All parameters are hardcoded:

| Parameter | Value |
|---|---|
| `cache_size` | 500 MB |
| Test duration | 5 seconds (hardcoded `sleep_for`) |
| `key_size` | 1 character |
| `value_size` | 2 characters |
| Collection name | `table:my_collection` |

## Test Scenarios

### Scenario: Single-threaded insert and read verification
- **What it tests:** Inserts one key-value pair (`"a"` → `"b"`) and verifies a search for a non-existent key (`"b"`) returns `WT_NOTFOUND`, then verifies searching for `"a"` succeeds.
- **Components:** `WT_CURSOR::insert`, `WT_CURSOR::search`.
- **Notes:** Basic functional check before spawning concurrent threads.

### Scenario: Concurrent random insert thread
- **What it tests:** Continuously inserts random 1-character keys with 2-character values until `do_inserts` is set to false (after 5 seconds).
- **Components:** `random_generator`, `WT_CURSOR::insert`.
- **Notes:** No transaction management; inserts are auto-committed (implicit transactions).

### Scenario: Concurrent random read thread
- **What it tests:** Continuously searches for random 1-character keys for 5 seconds. `WT_NOTFOUND` is silently ignored.
- **Components:** `WT_CURSOR::search`, `random_generator`.
- **Notes:** Uses `WT_IGNORE_RET` to suppress `WT_NOTFOUND` return values. No snapshot or timestamp management.

## Key Observations

- This test does not use the cppsuite test framework (`test` base class) and therefore has no config file, no metrics monitor, no operation tracker, and no validator.
- It is explicitly designed as a tutorial/template, as documented in the file header: "This file provides an example of how to create a test in C++ using a few features from the framework if any."
- The concurrent insert and read threads share the same collection via separate sessions and cursors but there is no explicit synchronization beyond the `do_inserts`/`do_reads` volatile-bool flags.
- There is no correctness validation beyond the initial single-threaded read check. The concurrent phase does not verify data integrity.
- This test is not suitable as a stress or regression test; its primary value is as documentation for new test authors.
