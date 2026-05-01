# test_compact12 — Compaction rewrites on-disk WT_REF_DELETED pages from fast truncate

**File:** `test/suite/test_compact12.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction subsystem, fast delete pages, checkpoint cleanup, statistics

## Test Cases

### `test_compact12.test_compact12_truncate`
- **What it tests:** Verifies that foreground compaction correctly rewrites on-disk pages in `WT_REF_DELETED` state (from fast truncation) and recovers at least 25% of the file size. Combines checkpoint cleanup (to remove obsolete 1/4 of rows) with compaction (to move fast-truncated 1/10 of rows from end of file).
- **Components:** `src/block/block_compact.c`, `src/btree/bt_delete.c`, `src/conn/conn_sweep.c`
- **Notes:** Skip: tiered. Currently skipped entirely due to `FIXME-SLS-1890` — "not robust to changes in eviction behavior". Inherits from both `compact_util` and `test_cc_base`. Populates 10 000 rows at ts=2, hard-deletes first 1/4 at ts=3, reopens connection (forces to disk), advances oldest to ts=4, fast-truncates last 1/10 at ts=5. Runs CC twice to remove obsolete content. Asserts `rec_page_delete_fast > 0` and `space_recovered > size_before // 4`.
