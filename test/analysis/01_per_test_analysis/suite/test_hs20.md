# test_hs20 — History store: reverse-modify not reconstructed from on-page overflow values

**File:** `test/suite/test_hs20.py`
**Storage mode:** General
**Components under test:** history store, modify, eviction, overflow values, checkpoint

## Test Cases

### `test_hs20.test_hs20`
- **What it tests:** Creates a table with `leaf_value_max=10B` to force all values exceeding 10 bytes to be stored as overflow items. Inserts 10 keys with 500-byte values (ts=2), applies two modifies at offset 500 and 501 (ts=3 and ts=4, appending characters). Inserts 100,000 additional rows to trigger eviction. Overwrites the original 10 keys at ts=5. Checkpoints (moving the overflow-based modify chain to HS while keeping the ts=5 on-disk image). Reads at ts=3 and verifies the value is `value1 + "B"`. Ensures that the HS reverse-modify code does not attempt to use the on-page overflow value as the base for reconstruction when the overflow item is no longer present.
- **Components:** `src/history/`, `src/modify/`, `src/evict/`, `src/block/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; cache_size=50MB,eviction=(threads_max=1); `rollbacks_allowed=5`. macOS systems may see `"Eviction took more than 1 minute"` (ignored). This test addresses the case where overflow items complicate HS reverse-delta reconstruction.
