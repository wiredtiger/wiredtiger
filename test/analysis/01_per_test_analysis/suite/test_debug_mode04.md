# test_debug_mode04 — Tests debug_mode eviction flag basic functionality

**File:** `test/suite/test_debug_mode04.py`
**Storage mode:** General
**Components under test:** debug mode, eviction

## Test Cases

### `test_debug_mode04.test_table_eviction`
- **What it tests:** Smoke test that `debug_mode=(eviction=true)` does not break normal data insertion (100 entries with binary values). No specific eviction behavior is asserted beyond absence of errors.
- **Components:** `src/evict/`, `src/conn/conn_debug.c`
- **Notes:** Flag causes aggressive eviction of pages after they are written; functional correctness is verified implicitly by the absence of exceptions.

### `test_debug_mode04.test_table_eviction_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(eviction=false)` via `conn.reconfigure()` disables the aggressive eviction flag without errors, and that normal insertions still succeed.
- **Components:** `src/evict/`, `src/conn/conn_debug.c`
- **Notes:** No statistical assertions; purely a no-crash check.
