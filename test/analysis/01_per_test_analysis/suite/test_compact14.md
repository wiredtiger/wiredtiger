# test_compact14 — Background compaction skips small files

**File:** `test/suite/test_compact14.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, file size threshold, statistics

## Test Cases

### `test_compact14.test_compact14`
- **What it tests:** Verifies that background compaction skips tables that are below the minimum file size threshold (files too small to benefit from compaction). A table with only 1 row should be skipped immediately.
- **Components:** `src/support/background_compact.c`
- **Notes:** Skip: tiered. Creates one table with 1 row (`table_numkv=1`), checkpoints, then enables background compaction. Polls until `get_bg_compaction_files_skipped() > 0`, confirming the tiny file was skipped. Tests the minimum-file-size guard in the background compaction server.
