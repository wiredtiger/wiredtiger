# test_rollback_to_stable16 — RTS removes on-disk updates for column and row store

**File:** `test/suite/test_rollback_to_stable16.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, column store, row store, crash recovery, in-memory RTS

## Test Cases

### `test_rollback_to_stable16.test_rollback_to_stable16`
- **What it tests:** Verifies RTS removes on-disk updates beyond the stable timestamp for both column and row store. Inserts 4 batches of 200 rows each at timestamps ts=2/5/7/9 (4 distinct row ranges). Sets stable=5. For on-disk: checkpoints and crash-restarts; for in-memory: calls RTS directly. Post-RTS: verifies rows at ts=2 (batch 1) and ts=5 (batch 2) are visible; rows at ts=7/9 (batches 3/4) are absent. Stats: `upd_aborted + keys_removed >= (nrows*2) - 2`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/col/`, `src/row/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), value_format (variable=S), in_memory, worker threads. Uses RTS verifier as teardown. `cache_size=200MB`. Comment in source notes this test may be somewhat redundant with others.
