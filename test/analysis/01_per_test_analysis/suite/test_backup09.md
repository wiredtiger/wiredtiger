# test_backup09 — Opening a backup cursor forces a log file switch

**File:** `test/suite/test_backup09.py`
**Storage mode:** General
**Components under test:** backup cursor, log file rotation, recovery

## Test Cases

### `test_backup09.test_backup_rotates_log`
- **What it tests:** Inserts 10 records (optionally checkpointed), then 10 more, confirming only 1 log file exists. Opens a backup cursor and asserts that exactly 2 log files now exist (the cursor open forced a switch). Inserts 10 more records after the cursor is open. Copies files to a backup directory either from the backup cursor only (1 log file copied) or via a full directory copy (2 log files). Opens the backup, verifies the recovered document count: backup-cursor-only restores up to `last_doc_in_backup`; full-copy restores all `last_doc_in_data` docs.
- **Components:** `src/cursor/cur_backup.c`, `src/log/log.c`, `src/conn/conn_open.c`
- **Notes:** Parametrized across 3 scenarios: (1) checkpoint + backup cursor only; (2) no checkpoint + backup cursor only; (3) checkpoint + copy all log files. Skips scenario 3 on Windows. Uses `transaction_sync=(enabled,method=none)` to avoid explicit log flushes during inserts.
