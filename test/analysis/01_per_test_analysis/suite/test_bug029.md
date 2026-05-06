# test_bug029 — WT-9457: checkpoint time propagation across restarts and backup

**File:** `test/suite/test_bug029.py`
**Storage mode:** General
**Components under test:** checkpoint clock, backup cursor, checkpoint retention

## Test Cases

### `test_bug029.test_bug029`
- **What it tests:** Reproduces WT-9457 where frequent checkpoints pushed the checkpoint wall-clock time forward, so that after a restart the backup cursor's checkpoint appeared older than the current time and was erroneously deleted, causing fatal read errors when restoring the backup. Inserts 2000 rows, then issues 100 forced checkpoints to advance the internal checkpoint time. Adds 2000 more rows and checkpoints again (creating pages that can be reused). Reopens the connection, inserts 100 more rows, opens a backup cursor, and issues 10 more forced checkpoints with additional data to try to overwrite the backup checkpoint's blocks. Completes the backup copy, then opens the backup directory and reads every 10th record to confirm the data is intact.
- **Components:** `src/checkpoint/checkpoint.c`, `src/backup/backup.c`, `src/block/block_mgr.c`
- **Notes:** Non-parametrized. Tagged `checkpoint:recovery`. 50 MB cache.
