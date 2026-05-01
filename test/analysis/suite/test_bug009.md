# test_bug009 — Reconciliation page split with prefix-compressed keys

**File:** `test/suite/test_bug009.py`
**Storage mode:** General
**Components under test:** reconciliation, prefix compression, page split

## Test Cases

### `test_bug009.test_reconciliation_prefix_compression`
- **What it tests:** Verifies that reconciliation correctly accounts for the size reduction introduced by prefix compression when deciding where to split pages. Creates a `file:` URI with 4 KB internal and leaf page sizes, `prefix_compression=1`, and `leaf_value_max=3096`. Inserts two keys (`fill_2__b_27` / `fill_2__b_28`) with values sized 2294 and 3022 bytes respectively — chosen so that their prefix-compressed representation tips the page size estimate over a boundary. The test passes if no crash or corruption occurs during reconciliation.
- **Components:** `src/btree/bt_rec.c`, `src/btree/rec_write.c`
- **Notes:** Non-parametrized. File URI only. Regression test for a specific crash in the prefix-compression size accounting during leaf page splits.
