# test_bug019 — Log pre-allocation: pre-allocated files must not accumulate

**File:** `test/suite/test_bug019.py`
**Storage mode:** General (logging enabled)
**Components under test:** log pre-allocation, log file rotation, statistics

## Test Cases

### `test_bug019.test_bug019`
- **What it tests:** Verifies that the WAL pre-allocation mechanism does not allow pre-allocated log files to accumulate indefinitely (a bug observed on Windows). Creates a table and inserts enough data (2000-byte values) to churn through many 100 KB log files. Checks `log_prealloc_max` and `log_prealloc_used` statistics at multiple points: (1) the pre-allocation count rises above the baseline during heavy inserts; (2) the pre-allocation range moves forward (used count increases) over 10 more insert rounds; (3) after traffic stops and the system is idle, the pre-allocation count drops below the high-water mark within 90 seconds.
- **Components:** `src/log/log_prealloc.c`, `src/log/log.c`
- **Notes:** Non-parametrized. `log=(enabled,file_max=100K)` and `statistics=(fast)`. Uses `time.sleep` waits bounded by 90 seconds.
