# test_prepare25 — Two sequential prepares with eviction: first rollback, second commit

**File:** `test/suite/test_prepare25.py`
**Storage mode:** General
**Components under test:** prepared transactions, eviction split, sequential prepare/rollback/commit, timing stress

## Test Cases

### `test_prepare25.test_prepare25`
- **What it tests:** Runs 1,000 iterations of: insert value_a, prepare value_b, evict with split failpoint, rollback first prepare; then prepare value_c, evict again (no failpoint), commit second prepare; verifies values at all timestamps (value_a at ts_a, value_c at ts_c, nothing visible at intermediate timestamps)
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `btree/bt_rec.c`
- **Notes:** No scenarios; uses `failpoint_eviction_split` for the first eviction attempt; after the first prepare is rolled back and the second is committed with a successful eviction, verifies the chain is correct: value_a in HS at earlier timestamp, value_c as current; guards against state corruption from the rollback+re-prepare pattern on the same key
