# test_rollback_to_stable15 — RTS handles updates in update-list for variable length column store (in-memory eviction disabled)

**File:** `test/suite/test_rollback_to_stable15.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, VLCS (variable length column store), in-memory update list

## Test Cases

### `test_rollback_to_stable15.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly handles the update-list path for VLCS (integer value format). With `debug_mode=(eviction=false)` all updates remain in memory update lists rather than being evicted. Writes 2,000 rows at ts=2 (value20=0x20), then at ts=5 (value30=0x40). Sets stable=2 and calls RTS: verifies only value20 visible. Then writes value30@7 and value40@9. Sets stable=7 and calls RTS: verifies only value30 visible. Stats: `calls=2`, `upd_aborted == (nrows*2) - 2`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/col/`, `src/evict/`
- **Notes:** Parametrized on key_format (column/row_integer), value_format (variable=integer `i`), in_memory (true/false), worker threads (0/4/8). No crash-restart — exercises runtime `rollback_to_stable()`. Uses RTS verifier (`verify_rts_logs`) as teardown action. `cache_size=200MB`. Note: `value30` variable is overwritten to `0x40` in source (looks like a bug in the test variable naming, but actual behavior is `value20=0x20`, `value30=0x40`).
