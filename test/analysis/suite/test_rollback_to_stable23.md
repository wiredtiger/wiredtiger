# test_rollback_to_stable23 — Search uses proper base update after RTS removes on-disk update

**File:** `test/suite/test_rollback_to_stable23.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, cursor search, modify operations, crash recovery

## Test Cases

### `test_rollback_to_stable23.test_rollback_to_stable`
- **What it tests:** Verifies that `cursor.search()` returns the correct value (reconstructed from HS modifies) after RTS has removed the on-disk update. Writes value_a@20, then modifies Q@30, R@40, S@50, T@60. Sets stable=50 (non-prepare) or stable=60 (prepare). Checkpoints and crash-restarts. Post-restart: uses `check_with_set_key` (explicit cursor.search() per key) to verify value_a at ts=20, modQ at ts=30, modR at ts=40, modS at ts=50. Stats: `hs_restore_updates == nrows`, `hs_removed >= nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/modify.c`, `src/cursor/`
- **Notes:** Parametrized on key_format (column/row_integer) and prepare. 1,000 rows. Differs from test_rollback_to_stable14 in that it uses explicit `cursor.search()` calls to verify reconstruction (not just a scan). `cache_size=50MB`.
