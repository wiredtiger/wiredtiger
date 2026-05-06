# test_prepare24 — Eviction split failure during prepare commit with verify

**File:** `test/suite/test_prepare24.py`
**Storage mode:** General
**Components under test:** prepared transactions, eviction split, commit, timing stress

## Test Cases

### `test_prepare24.test_prepare24`
- **What it tests:** Runs 1,000 iterations of: insert value_a, prepare value_b, attempt eviction (split fails due to `failpoint_eviction_split`), commit the prepare, evict again (this time succeeds), verify all timestamps show correct values
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_rec.c`
- **Notes:** No scenarios; uses `failpoint_eviction_split` timing stress; the key difference from test_prepare23 is that the prepare is committed (not rolled back) after the failed eviction; after the second (successful) eviction, verifies value_a is in the history store (readable at ts before prepare) and value_b is the current value; guards against state corruption from commit after a failed eviction
