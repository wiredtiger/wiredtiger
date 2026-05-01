# test_truncate29 — Verify handles timestamp usage checks correctly after timestamped fast truncate

**File:** `test/suite/test_truncate29.py`
**Storage mode:** General
**Components under test:** fast delete, verify, no-timestamp truncate, long-running reader, timestamp usage checks

## Test Cases

### `test_truncate29.test_truncate29`
- **What it tests:** Inserts 10,000 rows (large values) at ts=30 and ts=50; makes data globally visible (stable=oldest=50); reopens; opens a long-running reader that pins key 100; performs a no-timestamp (`no_timestamp=true`) full-table URI truncate; verifies at least one fast-delete page was produced; checkpoints; closes the long-running reader and rolls it back; calls `verifyUntilSuccess()` on the file. Tests that verify correctly handles timestamp usage checks after a no-timestamp fast truncate.
- **Components:** `btree.c`, `verify.c`, `txn.c`
- **Notes:** Integer-row format, file URI (`file:test_truncate29`). Regression test ensuring verify does not fail with incorrect timestamp usage errors when no-timestamp fast truncates are present.
