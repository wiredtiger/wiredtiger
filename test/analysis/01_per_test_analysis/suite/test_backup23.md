# test_backup23 — Backup restore incompatible with verify_metadata=true

**File:** `test/suite/test_backup23.py`
**Storage mode:** General
**Components under test:** backup cursor, connection open (verify_metadata), recovery

## Test Cases

### `test_backup23.test_backup23`
- **What it tests:** Creates a file, writes data (checkpoint + uncommitted data that will be recovered), takes a full backup, closes the original connection. Attempts to open the backup with `verify_metadata=true` and asserts that it raises `WiredTigerError` with `/restoring a backup is incompatible/`. Then opens the backup without `verify_metadata` and verifies all data (including the recovered post-checkpoint entries) matches the original.
- **Components:** `src/cursor/cur_backup.c`, `src/conn/conn_open.c`, `src/meta/meta_ckpt.c`
- **Notes:** Non-parametrized. Demonstrates that `verify_metadata` is incompatible with backup restore mode.
