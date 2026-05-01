# test_rollback_to_stable11 — RTS retrieves proper history store update across two crash-restart cycles

**File:** `test/suite/test_rollback_to_stable11.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, crash recovery, checkpoint

## Test Cases

### `test_rollback_to_stable11.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly retrieves the proper HS update with a 1-row table across two crash-restart cycles. First cycle: writes value_a at ts=12/14/16 and value_b at ts=20; stable=20 (non-prepare) or stable=28 (prepare); checkpoint then crash. Post-restart: value_b visible at ts=20. Second cycle: writes value_c at ts=30/32/34 and value_d at ts=36; checkpoint then crash. Post-restart: value_b still visible at ts=20 and ts=40. Stats after second restart: `calls=0`, `keys_removed=0`, `keys_restored=0`, `upd_aborted=0`, `hs_removed=4`, `hs_sweep=0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/log/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer) and prepare. Tiny `cache_size=1MB`. The `hs_removed=4` verifies exactly 4 HS entries were cleaned (the 3 value_a entries and the 1 value_c/value_d entries from the second set). RTS runs implicitly during recovery (`calls=0`).
