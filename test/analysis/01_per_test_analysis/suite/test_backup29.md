# test_backup29 — Incremental backup block bitmap correctness after dhandle reopen and sweep

**File:** `test/suite/test_backup29.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), block bitmap, data handle sweep, checkpoint

## Test Cases

### `test_backup29.test_backup29_reopen`
- **What it tests:** Sets up two tables with many rows and an incremental backup established (ID1). Closes and reopens the connection. Verifies that incremental backup stats are preserved (`backup_incremental=1`). Then modifies table 1 and checkpoints (keeping table 2 clean), then modifies table 2 and checkpoints again. Validates that the block mod bitmaps are correctly maintained across the reopen — specifically that bits set before the reopen are not cleared.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/block/block_ext.c`

### `test_backup29.test_backup29_sweep`
- **What it tests:** Same setup as reopen test, but instead of reopening the connection, waits for the file-handle sweeper to close all idle dhandles (except metadata, HS, lock, stats, and the active table). Verifies that when an uncached dhandle is reopened by a checkpoint, the block mod bitmap for the swept-away tables is correctly preserved. Uses `file_manager=(close_idle_time=3,close_scan_interval=1)` and polls for `stat.conn.file_open == 5` (final expected open count).
- **Components:** `src/cursor/cur_backup.c`, `src/conn/conn_dhandle.c`, `src/block/block_ext.c`
- **Notes:** Non-parametrized. Establishes ID1 backup without actually copying files. `nentries=5000`, `few=100`. Bitmap comparison function retained but currently commented out due to evolving behavior.
