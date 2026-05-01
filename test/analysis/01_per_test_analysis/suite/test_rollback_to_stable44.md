# test_rollback_to_stable44 — RTS backs out prepared transactions on recovery when stable timestamp is not set

**File:** `test/suite/test_rollback_to_stable44.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, prepared transactions, crash recovery

## Test Cases

### `test_rollback_to_stable44.test_rollback_to_stable`
- **What it tests:** Verifies that recovery-time RTS backs out a prepared (but uncommitted) transaction even when no stable timestamp was ever set. Writes value_a@10 to all 10 keys. Opens a second session, prepares a write of value_b to key 1 at prepare_ts=20 (never commits). Evicts the page. Checkpoints and crash-restarts. Post-recovery: 0 rows at ts=5, value_a at ts=15 and ts=25 for all rows (prepared txn was rolled back).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`, `src/evict/`
- **Notes:** Parametrized on key_format (column/row_integer). No stable/oldest timestamps set. `verbose=(rts:5)`. Eviction uses `ignore_prepare=true` to force the prepared page to disk. Unlike test_rollback_to_stable31 which tests regular writes, this specifically tests prepared txns with no stable timestamp.
