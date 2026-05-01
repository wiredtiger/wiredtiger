# test_log05 — Log file corruption recovery does not create duplicate log files

**File:** `test/suite/test_log05.py`
**Storage mode:** General (logging enabled: `log=(enabled=true)`)
**Components under test:** log recovery, log salvage, log corruption handling, disk space stability

## Test Cases

### `test_log05.test_duplicate_logs`
- **What it tests:** Injects corruption into log files by overwriting the first 4 bytes of a log record (the length field) with `0xFFFFFFFF` (UINT32_MAX), then reopens the connection 20 times. Verifies that each recovery cycle detects the corruption, invokes salvage, and completes successfully. Also verifies that the total number of log files never exceeds 2 (no unbounded log file creation).
- **Components:** `src/log/log.c`, `src/log/log_verify.c`, `src/log/log_salvage.c`, `src/os_posix/os_fs.c`
- **Notes:** `test_round = 20` iterations. Each cycle:
  1. Closes the connection cleanly.
  2. Reads `WiredTiger.turtle` to find `checkpoint_lsn` (file number, byte offset).
  3. Overwrites the 4-byte length field at that offset in the corresponding log file with `UINT32_MAX`.
  4. Reopens the connection; expects `"corrupted record length oversize at position"` in stdout.
  After 20 cycles, counts how many of the 20 expected log files (`WiredTigerLog.000000001` to `WiredTigerLog.000000020`) still exist on disk. Asserts the count is at most 2, validating that old log files are properly archived/removed and not duplicated.

  Initial data: 1000 key-value pairs (`key_i → val_i`) in a single committed transaction.
