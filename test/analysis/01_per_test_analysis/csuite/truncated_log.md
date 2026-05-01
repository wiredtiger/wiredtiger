# truncated_log — Log recovery with a mid-record truncation

**Path:** `test/csuite/truncated_log/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Log recovery, log cursor, partial log record handling, truncate system call

## What This Test Does
This test verifies that WiredTiger's log recovery correctly handles a log file that ends with a partial (truncated) record. A child process fills the database until the log transitions to log file 2, recording the LSN of the last complete record in log file 1. The parent process then truncates log file 1 in the middle of that last record (adding V_SIZE bytes past the record start), reopens the database with log recovery, and checks that all records up to (but not including) the truncated one are present. It then writes a new log record and walks the entire log to confirm the log cursor successfully skips the truncated data in log file 1 and reaches file 3.

## Test Scenarios / Cases

### Scenario: Row-store with mid-record log truncation (default)
- **What it tests:** That recovery ignores the partial record at the end of log file 1 and recovers all prior committed records. Also verifies that a log cursor can traverse beyond a truncated file.
- **Components:** Log recovery, `truncate()` system call, log cursor (`log:` URI), `session->log_printf`.
- **Notes:** The test explicitly checks that no record from log file 2 is visible via the log cursor (except the system record for the previous LSN).

### Scenario: Column-store variant (`-c` flag)
- **What it tests:** Same truncation-recovery behavior using a column-store table (`key_format=r`).
- **Components:** Column-store table, log recovery.

## LazyFS Variant
None.
