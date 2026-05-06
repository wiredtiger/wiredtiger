# test_backup18 — backup:query_id cursor: list incremental IDs, error cases, force_stop, crash

**File:** `test/suite/test_backup18.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), backup:query_id cursor, statistics, crash recovery

## Test Cases

### `test_backup18.test_backup18`
- **What it tests:** Tests the `backup:query_id` cursor API exhaustively: (1) error before any incremental backup is configured; (2) error if passed as a duplicate of an open backup cursor; (3) error while a backup cursor is open; (4) basic query after ID1 backup (returns ["ID1"]); (5) after ID2 and ID3 incremental backups, returns the last 2 IDs; (6) survives `reopen_conn` (IDs persist); (7) after `force_stop`, query raises error and stats confirm `backup_incremental=0`; (8) re-establishing incremental (ID1) works after force_stop; (9) after another force_stop and `simulate_crash_restart`, query still raises error and stats are zeroed. Checks `backup_cursor_open`, `backup_incremental`, and `backup_granularity` stats at key points.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/conn/conn_stat.c`
- **Notes:** Non-parametrized. Default granularity = 16 MB. Uses `simulate_crash_restart`.
