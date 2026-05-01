# test_backup20 — Incremental backup force_stop without prior checkpoint (WT-7027 regression)

**File:** `test/suite/test_backup20.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), force_stop, session isolation

## Test Cases

### `test_backup20.test_backup20`
- **What it tests:** Opens an incremental backup cursor (ID1), closes it, then immediately issues `force_stop=true` without taking any checkpoint in between. This reproduces the WT-7027 scenario where snapshot-isolation sessions caused an assertion. Verifies the operation completes without error and the connection closes cleanly.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/txn/txn.c`
- **Notes:** Parametrized across session isolation: default, read-committed, read-uncommitted, snapshot.
