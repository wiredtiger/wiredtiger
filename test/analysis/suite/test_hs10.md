# test_hs10 — History store: modify read correctness after eviction

**File:** `test/suite/test_hs10.py`
**Storage mode:** General
**Components under test:** history store, modify, eviction, cursor reads

## Test Cases

### `test_hs10.test_modify_insert_to_hs`
- **What it tests:** Inserts a 1,000-byte base value (ts=2), applies 3 modifies in separate transactions (ts=3, 4, 5), checkpoints. Uses a second table with 10,000 inserts to pressure eviction of the first table's pages out of cache. Then reads at ts=3, 4, and 5 and verifies the expected values (`value1+'A'`, `value1+'AB'`, `value1+'ABC'`) are correctly reconstructed from the history store after eviction.
- **Components:** `src/history/`, `src/modify/`, `src/evict/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; `cache_size=2MB,statistics=(all),eviction=(threads_max=1)`. Uses a separate session2 with a separate table for eviction pressure. Demonstrates that modifies stored in HS can be correctly assembled after the original page is evicted.
