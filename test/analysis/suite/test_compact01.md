# test_compact01 — Foreground compaction via session method and wt utility

**File:** `test/suite/test_compact01.py`
**Storage mode:** General (skips `timestamp` hook)
**Components under test:** compaction subsystem, block manager, statistics

## Test Cases

### `test_compact.test_compact`
- **What it tests:** Verifies that foreground compaction (`session.compact()` or the `wt compact` utility) reduces the number of on-disk btree leaf pages after deleting ~99% of rows from a simple or complex table, and that progress statistics (`pages_reviewed`, `pages_rewritten`, `pages_skipped`) are consistent.
- **Components:** `src/block/block_compact.c`, `src/session/session_compact.c`, `src/btree/bt_compact.c`
- **Notes:** Skip: `@wttest.skip_for_hook("timestamp", ...)`. Six scenarios from cross-product of types (file/SimpleDataSet, table/ComplexDataSet) × compact method (method, method_reopen, utility). `free_space_target=1MB`. Page size `leaf_page_max=8KB` balanced to avoid overflow items while creating enough pages. After compaction, asserts `btree_row_leaf < maxpages`. For method without reopen also checks `pages_rewritten + pages_skipped == pages_reviewed` stat invariant. Skips stat check for tiered and utility scenarios (utility resets connection stats).
