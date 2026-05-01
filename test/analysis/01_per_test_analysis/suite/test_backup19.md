# test_backup19 — Incremental backup with src_id-only initial backup

**File:** `test/suite/test_backup19.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based)

## Test Cases

### `test_backup19.test_backup19`
- **What it tests:** Creates a table, inserts an initial data set, takes a full incremental backup (ID "initial"), inserts more data, checkpoints, takes a full backup for comparison, takes an incremental backup, and validates the incremental matches the full backup. Specifically tests the scenario where the source ID was established by a cursor that did not specify a `src_id` (initial seeding case).
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`
- **Notes:** Non-parametrized. Same hotspot data pattern as test_backup15 (early bulk inserts, then single-key updates). Uses `WT_BLOCK` / `WT_BLOCK_LOG_FULL` / `WT_BLOCK_LOG_INCR` directories.
