# test_compact11 — Background compaction does not clear incremental backup block modification bits

**File:** `test/suite/test_compact11.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, incremental backup, block modification bitmap

## Test Cases

### `test_compact11.test_compact11`
- **What it tests:** Verifies that background compaction does not clear the block modification bitmaps (used for incremental backup) while it is working on tables. Incremental backups taken during compaction must contain correct block-level diff information, and comparing them against a full backup must show identical table contents.
- **Components:** `src/support/background_compact.c`, `src/backup/backup_incr.c`, `src/block/`
- **Notes:** Skip: tiered. Creates 5 tables with 100 000 rows, takes a full backup as the incremental base, inserts the latter 50%, deletes 50%. Then enables background compaction with `run_once=true`. While compaction runs, takes incremental backups each time `bytes_recovered` changes (using `bkup_id` counter). After compaction, compares all incremental backups against a pre-compaction full backup for each table using `compare_backups`. Uses `parse_blkmods` to read block modification bitmap from metadata cursor.
