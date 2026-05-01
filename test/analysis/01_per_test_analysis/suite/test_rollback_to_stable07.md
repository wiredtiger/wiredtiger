# test_rollback_to_stable07 — RTS after crash and recovery with two restart cycles

**File:** `test/suite/test_rollback_to_stable07.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, crash recovery, history store, transactions, checkpoint

## Test Cases

### `test_rollback_to_stable07.test_rollback_to_stable`
- **What it tests:** Verifies RTS behavior through two simulated crash-restart cycles. Writes 1,000 rows at ts=20 (value_d), 30 (value_c), 40 (value_b), 50 (value_a). Sets stable=40 (non-prepare) or stable=50 (prepare). Then writes value_b@60, value_c@70, value_d@80 past stable, and checkpoints. First crash restart: verifies value_b seen at ts=40/80, value_c at ts=30, value_d at ts=20; `hs_removed >= nrows*4`. Second crash restart: same data visible; `hs_removed=0` and `upd_aborted=0` since no further cleanup needed.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/log/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer) and prepare (true/false). `cache_size=5MB`. Both restarts use `simulate_crash_restart`. After first restart `calls=0` (RTS run during recovery, not explicitly).
