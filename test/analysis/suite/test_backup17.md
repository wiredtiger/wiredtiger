# test_backup17 — Incremental backup consolidate option collapses adjacent blocks

**File:** `test/suite/test_backup17.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based), consolidate option

## Test Cases

### `test_backup17.test_backup17`
- **What it tests:** Takes a full incremental backup (ID1, granularity 100 KB). Adds data to table 1 and takes incremental (ID1→ID2) **without** consolidation; asserts no block length exceeds the 100 KB granularity. Adds identical data to table 2 and takes incremental (ID2→ID3) **with** `consolidate=true`; asserts at least one block length exceeds granularity (adjacent blocks merged). Compares the two result sets: the consolidated backup has fewer length entries but the same approximate total bytes (within 2 × granularity tolerance).
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`
- **Notes:** Non-parametrized. gran=100 KB. Uses 1000-op inserts with 300-byte key and 500-byte value.
