# test_bug014 — WT-2115: fast-delete pages incorrectly lost after crash

**File:** `test/suite/test_bug014.py`
**Storage mode:** General
**Components under test:** fast-delete (fast truncate), crash recovery, checkpoint

## Test Cases

### `test_bug014.test_bug014`
- **What it tests:** Reproduces WT-2115 where fast-deleted pages could be lost across a crash. Populates a table with 1000 records on 512-byte pages, reopens so pages are on disk, then starts a transaction that fast-truncates records 250–500. While that truncation is uncommitted, takes an explicit checkpoint from a separate session. Simulates a crash by copying the database directory to `RESTART/`. Opens the restart directory and verifies that all 1000 records are still present (the uncommitted truncation must not be reflected in recovery).
- **Components:** `src/btree/bt_delete.c`, `src/checkpoint/checkpoint.c`, `src/conn/conn_recover.c`
- **Notes:** Parametrized across `column` (`key_format=r`) and `row_string` (`key_format=S`). Uses `copy_wiredtiger_home` to simulate crash.
