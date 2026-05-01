# test_compact07 — Background compaction selects tables by free-space threshold; foreground on remaining

**File:** `test/suite/test_compact07.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, free_space_target selection, foreground compaction, statistics

## Test Cases

### `test_compact07.test_compact07`
- **What it tests:** Verifies that background compaction with a specific `free_space_target` only compacts tables whose available free space exceeds the threshold, skipping smaller tables. Also verifies that foreground compaction can subsequently compact the skipped smaller table, and that dropped tables are eventually removed from the background compaction tracking list.
- **Components:** `src/support/background_compact.c`, `src/session/session_compact.c`, `src/block/block_compact.c`
- **Notes:** Skip: tiered. Creates one "small" table (20% deleted) and two "large" tables (90% deleted). Background compaction with threshold above small-table free space skips it (`session_table_compact_skipped > 0`) and compacts the large tables (`background_compact_success > 0`). Then foreground compaction is run on the small table. After restart, waits for `background_compact_files_tracked` to decrement after table drops. Uses `debug_mode=(background_compact)`.
