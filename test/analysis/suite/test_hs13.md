# test_hs13 — History store: reverse modify traversal after eviction

**File:** `test/suite/test_hs13.py`
**Storage mode:** General
**Components under test:** history store, modify (prepend/reverse delta), eviction

## Test Cases

### `test_hs13.test_reverse_modifies_constructed_after_eviction`
- **What it tests:** Inserts a 10,000-character base value for key 1. Applies a first modify (prepend 'A' at offset 0, extend). Session2 reads and verifies value = `'A' + value1`. Session2 begins a new transaction. A second modify prepends 'B' at offset 1, then a full-value update (value2) replaces key 1. Evicts the page using a `debug=(release_evict)` cursor. Session2 (with its older snapshot) re-reads and must reconstruct `'A' + value1` from the history store, requiring reverse-modify traversal (walking forward through HS from the newest full update backward through reverse deltas to reconstruct the queried snapshot).
- **Components:** `src/history/`, `src/modify/`, `src/evict/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; `cache_size=2MB,eviction=(threads_max=1)`. Exercises the "walk forward through HS to find base, then apply reverse deltas" code path.
