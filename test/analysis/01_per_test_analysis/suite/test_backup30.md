# test_backup30 — Query backup_checkpoint timestamp via conn.query_timestamp

**File:** `test/suite/test_backup30.py`
**Storage mode:** General
**Components under test:** backup cursor, timestamps, connection query API

## Test Cases

### `test_backup30.test_backup30`
- **What it tests:** Writes timestamped data, sets stable_timestamp=10, checkpoints. Queries `get=backup_checkpoint` before opening a backup cursor (expects "0"). Opens a backup cursor and queries again (expects the stable timestamp at the checkpoint, i.e., "a" = 10). Advances stable_timestamp=20, checkpoints again while cursor is still open; verifies `backup_checkpoint` still returns the original stable ts (pinned at cursor open time). Closes cursor (returns to "0"), reopens cursor (now reflects the new stable ts=20).
- **Components:** `src/cursor/cur_backup.c`, `src/txn/txn_timestamp.c`
- **Notes:** Non-parametrized. Validates that `get=backup_checkpoint` is pinned for the lifetime of the backup cursor.
