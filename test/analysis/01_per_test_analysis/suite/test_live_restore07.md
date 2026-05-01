# test_live_restore07 — Restoring from an empty source database fails

**File:** `test/suite/test_live_restore07.py`
**Storage mode:** General (Unix only)
**Components under test:** live restore initialization, error handling

## Test Cases

### `test_live_restore07.test_live_restore07`
- **What it tests:** Verifies that opening a live restore connection when the source directory is completely empty fails with a descriptive error.
- **Components:** `src/live_restore/live_restore_init.c`, `src/conn/conn_open.c`
- **Notes:** Parameterized by key format: `column` (`key_format='r'`) or `row_integer` (`key_format='i'`) — though key format has no effect since no data is created. Creates empty SOURCE and DEST directories without any WiredTiger files, then attempts `live_restore=(enabled=true,path="SOURCE")`. Expected error: `"Source directory is empty. Nothing to restore!"`.
