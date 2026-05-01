# test_backup13 — Incremental backup force_stop and crash recovery clears backup state

**File:** `test/suite/test_backup13.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), force_stop, crash recovery, session isolation

## Test Cases

### `test_backup13.test_backup13`
- **What it tests:** Takes a full incremental backup (ID1), adds more data, takes an incremental backup (ID1→ID2). Then issues `force_stop=true` to release incremental resources. Verifies that after force_stop, opening a cursor with old `src_id="ID1"` raises `WiredTigerError`. Simulates a crash (`simulate_crash_restart`) and verifies the same old-ID error occurs. Reopens normally and verifies the same. Also verifies that `read-committed` and `read-uncommitted` session isolation modes reject the timestamped writes with the expected error message.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/conn/conn_open.c`
- **Notes:** Parametrized across session isolation: default, read-committed, read-uncommitted, snapshot. read-committed/read-uncommitted are expected to fail on timestamped adds.
