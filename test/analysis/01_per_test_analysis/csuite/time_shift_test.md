# time_shift_test — Monotonic clock validation via libfaketime

**Path:** `test/csuite/time_shift_test.sh`
**Language:** Shell script (drives `test_rwlock` binary)
**Storage mode:** N/A
**Jira ticket:** N/A
**Components under test:** Monotonic clock usage inside `rwlock`, `libfaketime`

## What This Test Does
This shell script verifies that WiredTiger's internal code uses a monotonic clock (not `CLOCK_REALTIME`). It runs the `test_rwlock` binary once to measure its normal execution time, then re-runs it under `libfaketime` with the system clock shifted backwards by that same duration. If WiredTiger used `CLOCK_REALTIME` the second run would hang (waiting for a time that has already passed). The test passes if the second run completes within 20% of the original runtime.

## Test Scenarios / Cases

### Scenario: Normal execution baseline
- **What it tests:** Establishes the wall-clock duration of `test_rwlock` without any clock manipulation.
- **Components:** `test_rwlock` binary, `taskset` (Linux) for CPU affinity.
- **Notes:** `DONT_FAKE_MONOTONIC=1` is set so libfaketime does not affect the monotonic clock used by the baseline.

### Scenario: Negative time shift via libfaketime
- **What it tests:** That `test_rwlock` completes in approximately the same time when the real-time clock is shifted backwards by the baseline duration, confirming that WiredTiger does not block on `CLOCK_REALTIME`.
- **Components:** `libfaketime`, `~/.faketimerc` configuration, `DONT_FAKE_MONOTONIC=1`.
- **Notes:** The script writes a negative offset to `~/.faketimerc` and cleans it up after the run.

## LazyFS Variant
None.
