# test_layered70 — Skip writing full pages when reconciliation makes no stable progress

**File:** `test/suite/test_layered70.py`
**Storage mode:** Disagg/Layered
**Components under test:** Checkpoint skip-write optimization, `rec_page_full_image_leaf` stat, stable timestamp gating

## Test Cases

### `test_layered70.test_skip_write_full_page`
- **What it tests:** With leaf page deltas disabled (`leaf_page_delta=false`), verifies that `rec_page_full_image_leaf` is 0 when an insert is present but its commit_timestamp (ts=10) exceeds the current stable_timestamp (ts=1). Two checkpoint calls with stable still below the commit confirm the stat stays 0. Then stable is advanced to ts=10, and the next checkpoint produces `rec_page_full_image_leaf=1`. The same pattern is repeated for a second update (ts=20): two checkpoints with stable=10 yield 0, then stable=20 yields 1.
- **Components:** `src/btree/bt_rec.c`, disagg reconciliation skip-write logic, `src/conn/conn_ckpt.c`
- **Notes:** Tests the "no progress" optimization: if no new data has become stable since the last write, the page is not re-written to the page log. `layered:` URI. Uses `precise_checkpoint=true`. Disagg-only.
