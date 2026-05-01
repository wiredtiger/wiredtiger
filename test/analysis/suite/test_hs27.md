# test_hs27 — History store: VLCS does not RLE-compact adjacent keys with heterogeneous timestamps

**File:** `test/suite/test_hs27.py`
**Storage mode:** General (column-store only; no row-store scenarios)
**Components under test:** history store, variable-length column store, RLE encoding, eviction, cursor_prev

## Test Cases

### `test_hs27.test_hs`
- **What it tests:** Optionally initializes nrows=100 keys with value_1. Creates a long-running reader at ts=2. Writes value_2 to small batches of keys (nkeys=1,2,3) at different timestamps, in configurable order (groups forward/backward, keys within group forward/backward). Forces eviction of the first and last key. Validates data in three ways for each relevant read timestamp:
  1. `check1`: Reads specific keys individually to verify exact expected values.
  2. `check2`: Full forward scan verifies count and values.
  3. `check3`: Full backward scan (cursor_prev) verifies count and values.
  
  The key correctness property: adjacent keys written at different timestamps must not be merged into the same RLE group on disk, because merging would prevent reading the correct per-key version from HS.
- **Components:** `src/history/`, `src/column/`, `src/evict/`
- **Notes:** 96 scenarios (ntimes ∈ {2,3,10} × nkeys ∈ {1,2,3} × doinit ∈ {T,F} × group_forward ∈ {T,F} × keys_forward ∈ {T,F}). Uses `SimpleDataSet` with `key_format='r'`. `checkall()` validates at ts=2, at each write timestamp's read-time slot, and at ts=100.
