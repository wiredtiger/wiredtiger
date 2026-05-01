# test_hs06 — History store: HS lookup correctness and memory efficiency for reads, modifies, prepares

**File:** `test/suite/test_hs06.py`
**Storage mode:** General
**Components under test:** history store, modify, prepared transactions, checkpoint, eviction, memory usage

## Test Cases

### `test_hs06.test_hs_reads`
- **What it tests:** Writes 2,000 rows at timestamp 2 and again at timestamp 3. Checkpoints at stable_timestamp=2. Verifies checkpoint cursor reads value1 at the stable timestamp. Measures non-page memory before and after a full table scan at read_timestamp=2 (requiring HS lookup), asserting memory usage does not double. Confirms HS values are returned directly without building full in-memory update chains.
- **Components:** `src/history/`, `src/checkpoint/`, `src/evict/`
- **Notes:** Also tests checkpoint cursor with explicit `debug=(checkpoint_read_timestamp=...)` options including reading unstable values and timestamp=0 for most-recent.

### `test_hs06.test_hs_modify_reads`
- **What it tests:** Tests WT-5336 regression: inserts a base value at ts=2, applies modify at ts=3 (replace byte at 100), modify at ts=4 (replace byte at 200), then a full update at ts=5 (completely different). Checkpoints. Reads at ts=3 (should reconstruct from HS: needs forward scan to find newest full update at ts=4, then apply ts=3 reverse delta). Reads at ts=4 (direct HS full update). Verifies exact expected strings.
- **Components:** `src/history/`, `src/modify/`

### `test_hs06.test_hs_prepare_reads`
- **What it tests:** Inserts committed data, then prepares updates for rows 1–10 (leaving them in prepared state). Forces eviction via additional inserts. Reads at ts=3 (prepared time): verifies `WT_PREPARE_CONFLICT` for rows 1–10. Commits the prepared transaction, then verifies both commit_timestamp and durable_timestamp reads see the new values.
- **Components:** `src/history/`, `src/txn/`, `src/evict/`

### `test_hs06.test_hs_multiple_updates`
- **What it tests:** Two updates to the same key within one transaction at the same timestamp. Forces eviction. Verifies that reading at that timestamp returns the second (last) value, not the first — confirming correct HS handling of same-txn same-ts duplicate updates.
- **Components:** `src/history/`, `src/txn/`

### `test_hs06.test_hs_multiple_modifies`
- **What it tests:** Three modifies in one transaction (ts=3) at offsets 100, 200, 300. Overwrites with a full value (ts=4). Forces eviction. Reads at ts=3 and verifies all three modify offsets are reflected correctly.
- **Components:** `src/history/`, `src/modify/`

### `test_hs06.test_hs_instantiated_modify`
- **What it tests:** When stable_timestamp=1, no birthmark record is created during HS eviction. Applies three modifies (ts=3, 4, 5) on top of a base insert (ts=2), then flushes via a second table. Reads at ts=5 and verifies all three modifies applied correctly, confirming that HS instantiation converts the most-recent-modify into a standard update.
- **Components:** `src/history/`, `src/evict/`, `src/modify/`

### `test_hs06.test_hs_modify_stable_is_base_update`
- **What it tests:** Same as above but stable_timestamp=1 forces the base update behind stable. Three modifies (ts=3, 4, 5) are applied. Forces eviction. Reads at ts=5 and verifies the combined result.
- **Components:** `src/history/`, `src/evict/`, `src/modify/`

### `test_hs06.test_hs_rec_modify`
- **What it tests:** Applies three modifies (ts=3, 4, 5), then a full update (ts=6), then sets stable_timestamp=5 and checkpoints. The checkpoint must select the ts=5 modify from HS and unflatten it (instantiate the full value) for reconciliation. Reads at ts=5 and verifies the combined modify result.
- **Components:** `src/history/`, `src/checkpoint/`, `src/modify/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`, `S`}; nrows=2000; cache_size=50MB.
