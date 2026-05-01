# test_hs28 — History store: full update instead of reverse modify when modify follows squashed on-page value

**File:** `test/suite/test_hs28.py`
**Storage mode:** General
**Components under test:** history store, modify, checkpoint, statistics (cache_hs_insert_full_update, cache_hs_insert_reverse_modify)

## Test Cases

### `test_hs28.test_insert_hs_full_update`
- **What it tests:** Inserts a base value "a" at ts=2. Applies a modify at ts=5 (replace byte 0 with 'A'). Commits a transaction with two updates for the same key (ts=10): value1 followed by value2. This double-update creates a "squashed" on-page value (only the last update in a transaction is kept). Checkpoints. Asserts:
  - `cache_hs_insert_full_update == 2` (two full updates were inserted into HS: the ts=2 value1 and the ts=5 'A'+value1 result)
  - `cache_hs_insert_reverse_modify == 0` (no reverse-modify entries were created)
  
  Tests the rule: when a modify follows a squashed on-page value (i.e., the preceding on-page update was the result of squashing), a full update (not a reverse delta) must be written to HS, because there is no reliable base for delta reconstruction.
- **Components:** `src/history/`, `src/modify/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; no explicit cache_size (uses connection-level `conn_config` returning `'cache_size=50MB,...'`). The `conn_config` is defined as a method (overriding the class attribute that also exists) — the method version takes precedence.
