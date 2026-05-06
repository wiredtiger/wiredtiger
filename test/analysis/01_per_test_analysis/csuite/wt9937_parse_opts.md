# wt9937_parse_opts — Test framework option parsing unit test

**Path:** `test/csuite/wt9937_parse_opts/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-9937
**Components under test:** `testutil_parse_opts`, `testutil_parse_single_opt`, `testutil_parse_begin_opt`, `testutil_parse_end_opt`, `TEST_OPTS`, tiered storage option parsing

## What This Test Does
This test is a unit test for the WiredTiger test framework's option parsing infrastructure. It exercises both `testutil_parse_opts` (the standard all-in-one option parser) and the extended parsing idiom using `testutil_parse_begin_opt` + `__wt_getopt` + `testutil_parse_single_opt` + `testutil_parse_end_opt`. It runs a table of 14 test cases against simulated command lines covering string options in both `-b option` and `-boption` forms, integer options in `-T 21` and `-T21` forms, tiered storage multi-character options (`-PT`, `-Po name`, `-PSE2345,D1234`), verbose flags, and a fictional test program's own options (`-c`, `-d`, `-e`, `-f`). Each test case asserts that the parsed `TEST_OPTS` and `FICTIONAL_OPTS` fields exactly match expected values.

## Test Scenarios / Cases

### Scenario: Standard `testutil_parse_opts` — single and compound option forms
- **What it tests:** That `testutil_parse_opts` correctly parses `-b` (build dir), `-T` (thread count), `-v` (verbose), and `-P` (tiered storage) options in both concatenated and space-separated forms, including random-seed initialization from `-PSE`/`-PSD` sub-options.
- **Components:** `testutil_parse_opts`, `TEST_OPTS` fields: `build_dir`, `nthreads`, `verbose`, `tiered_storage`, `tiered_storage_source`, `data_seed`, `extra_seed`.
- **Notes:** 9 test cases using `argv[0]="parse_opts"`. NONZERO sentinel (0xfafafafa) used to assert any non-zero seed value without checking exact value.

### Scenario: Extended parsing with `testutil_parse_single_opt` and fictional options
- **What it tests:** That a test program can parse its own options (`-c string`, `-d flag`, `-e flag`, `-f int`) while delegating standard options (`-b`, `-P`, `-T`, `-v`) to `testutil_parse_single_opt`, using the `testutil_parse_begin_opt` idiom, without interfering with each other.
- **Components:** `testutil_parse_begin_opt`, `__wt_getopt`, `testutil_parse_single_opt`, `testutil_parse_end_opt`, `FICTIONAL_OPTS` struct (`checkpoint_name`, `delete_flag`, `energize_flag`, `fuzziness_option`).
- **Notes:** 5 test cases using `argv[0]="parse_single_opt"`. Demonstrates option override: fictional `-d` shadows the standard `-d` option.

## LazyFS Variant
None.
