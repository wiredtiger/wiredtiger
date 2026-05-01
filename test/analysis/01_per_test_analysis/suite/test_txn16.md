# test_txn16 — Recovery: toggling logging on/off does not accumulate spurious log files

**File:** `test/suite/test_txn16.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** logging toggle, log file lifecycle, recovery

## Test Cases

### `test_txn16.test_recovery`
- **What it tests:** Populates 3 tables with 1,000 rows and varying checkpoints; copies the database to a RESTART directory; closes the connection; then for both the original and RESTART directories runs `run_toggle` 3 times: (1) opens with logging on to run recovery; (2) records current log files; (3) removes all log files; (4) opens with logging off. Verifies that after each cycle, the new log files never overlap with the original log files (i.e., no regression back to log file 1), and that repeated open/close with the same config produces the same set of log files.
- **Components:** `log.c`, `recovery.c`, `meta.c`
- **Notes:** No parameterization. Tests that toggling logging enabled/disabled across connections does not cause log file numbering to restart or accumulate unexpectedly.
