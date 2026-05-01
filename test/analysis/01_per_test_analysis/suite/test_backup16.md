# test_backup16 — Incremental backup file selection: only modified files appear in incremental

**File:** `test/suite/test_backup16.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based), file-level change tracking

## Test Cases

### `test_backup16.test_backup16`
- **What it tests:** Creates 6 tables. Performs a simulated full backup (ID0, no actual copy). Then adds data selectively to some tables and checkpoints. Validates which files are included in the first incremental (ID0→ID1): expects only the tables that changed (test1, test4 [new, no checkpoint], test5 [new with data]). Next round adds data to test3 and test5; incremental (ID1→ID2) should include only test3, test4, and test5. Final incremental (ID2→ID3) with no changes should yield only test4 (never checkpointed, always appears). Asserts that only files with actual block changes return data from the incremental duplicate cursor and that the byte count is nonzero.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/block/block_mgr.c`
- **Notes:** Non-parametrized. Validates the distinction between "new file created after full backup" (full copy every time) vs "existing file with no changes" (never appears after first checkpoint).
