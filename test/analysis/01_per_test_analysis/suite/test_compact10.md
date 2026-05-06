# test_compact10 — Background compaction does not alter data (full backup comparison)

**File:** `test/suite/test_compact10.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, data integrity, backup comparison

## Test Cases

### `test_compact10.test_compact10`
- **What it tests:** Verifies that background compaction is a purely physical operation that does not alter the logical contents of tables. Takes a full backup before and after background compaction and compares the data in each table using `compare_backups`.
- **Components:** `src/support/background_compact.c`, `src/block/block_compact.c`, `src/backup/`
- **Notes:** Skip: tiered. Creates 5 tables, each with 100 000 rows, then deletes 50% of each. Takes `backup_1` before compaction. Enables background compaction with `run_once=true,free_space_target=1MB`. Waits for `background_compact_success >= num_tables`. Takes `backup_2`. Calls `compare_backups(uri, backup_1, backup_2)` for each table. Asserts `bytes_recovered > 0`. Tests the data-safety guarantee of background compaction.
