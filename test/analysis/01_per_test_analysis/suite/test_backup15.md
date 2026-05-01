# test_backup15 — Incremental backup correctness with hotspot key updates

**File:** `test/suite/test_backup15.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based), block bitmap tracking

## Test Cases

### `test_backup15.test_backup15`
- **What it tests:** Populates a table with a large initial dataset (100 000 records), then in subsequent iterations rapidly updates a single "hotspot" key to create a concentrated dirty block. Alternates the order of full vs. incremental backup between even/odd iterations. Compares full and incremental backups after each iteration to verify they are equivalent. Tests that the incremental block bitmap correctly tracks hotspot updates.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/block/block_mgr.c`
- **Notes:** Non-parametrized. 5 max iterations. After the first two data inserts, subsequent inserts update only the saved `savekey`. Uses `WT_BLOCK` / `WT_BLOCK_LOG_FULL` / `WT_BLOCK_LOG_INCR` directories.
