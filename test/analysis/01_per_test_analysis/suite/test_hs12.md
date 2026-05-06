# test_hs12 — History store: append modifies to string values (prepend and append)

**File:** `test/suite/test_hs12.py`
**Storage mode:** General
**Components under test:** history store, modify (append/prepend), eviction

## Test Cases

### `test_hs12.test_modify_append_to_string`
- **What it tests:** Inserts two keys with a 130-character base value. Applies a modify that appends 'A' at offset 130 (past end of string, extend) to key 1, and a modify that prepends 'AB' at offset 0 (insert before existing, extend) to key 2. Verifies via a second session that the values are `value1+'A'` and `'AB'+value1`. Then opens a new transaction in session2, inserts a new value for key 1, evicts the page, and re-reads via the older session2 transaction. Verifies session2 still sees the modify-extended value (`value1+'A'`) after eviction forces a HS lookup.
- **Components:** `src/history/`, `src/modify/`, `src/evict/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; `cache_size=2MB,eviction=(threads_max=1)`. The key insight tested: modifies that extend a string beyond its current length (zero-length replacement) are correctly stored in and reconstructed from the history store.
