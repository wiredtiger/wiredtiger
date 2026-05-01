# test_hs19 — History store: correct reverse-modify reconstruction after a modify-chain with OOO insert

**File:** `test/suite/test_hs19.py`
**Storage mode:** General
**Components under test:** history store, modify, eviction, checkpoint, reverse-delta reconstruction

## Test Cases

### `test_hs19.test_hs19`
- **What it tests:** A regression test for incorrect HS reverse-delta reconstruction when a non-contiguous modify (append without replacement) is present in the update chain above the one being reconstructed. Sequence:
  1. Insert key 1 without timestamp (value1).
  2. Two modifies: replace byte at 100 (ts=2), replace byte at 101 (ts=3).
  3. Pin oldest transaction via session2 with a large insert in junk table (to prevent global visibility).
  4. Insert a 10-byte append modify at offset 102 (ts=4, no replacement — extends string).
  5. Insert a 1-byte modify at offset 102 (ts=5, replaces).
  6. Checkpoint at stable_timestamp=5 (writes the ts=5 modify as on-disk value and older values to HS).
  7. Add one more modify at ts=6 to mark the page dirty.
  8. Evict the page.
  
  Reads at ts=2, ts=3, and ts=4 and verifies exact expected byte sequences, confirming that the append modify at ts=4 does not corrupt the reconstruction of earlier timestamps.
- **Components:** `src/history/`, `src/modify/`, `src/evict/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; cache_size=5MB,eviction=(threads_max=1). The bug being tested: the append-at-102 modify was previously used unintentionally to reconstruct the final value, corrupting the result.
