# test_rollback_to_stable02 — RTS restores history store value to replace on-disk value

**File:** `test/suite/test_rollback_to_stable02.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, transactions, checkpoint

## Test Cases

### `test_rollback_to_stable02.test_rollback_to_stable`
- **What it tests:** Verifies that RTS replaces the on-disk (post-stable) value with the correct value from the history store. Writes 10,000 rows at ts=10 (value_a), 20 (value_b), 30 (value_c), 40 (value_d). Sets stable=20 (non-prepare) or stable=30 (prepare). After checkpoint and RTS, verifies that the latest visible value is value_b (not value_d). In dryrun mode value_d remains visible. Stats: `calls=1`, `keys_removed=0`, `keys_restored=0`; `upd_aborted + hs_removed >= nrows*2` (non-dryrun); `upd_aborted_dryrun + hs_removed_dryrun >= nrows*2` (dryrun).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, dryrun, worker threads (0/4/8). Prepare shifts stable by +10 (stable_timestamp=30 instead of 20). `extraconfig` is an empty string; subclasses may override. `cache_size=100MB`.
