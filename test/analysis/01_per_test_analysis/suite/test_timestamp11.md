# test_timestamp11 — Mixed timestamped and non-timestamped writes

**File:** `test/suite/test_timestamp11.py`
**Storage mode:** General
**Components under test:** `no_timestamp=true` transactions, rollback_to_stable, history store

## Test Cases

### `test_timestamp11.test_timestamp_range`
- **What it tests:** Writes key1 and key2 at timestamp=2; updates key1 at timestamp=5 (timestamped) and key2 with `no_timestamp=true`; sets stable=2, checkpoints, calls rollback_to_stable; verifies key1 rolled back to value2, key2 retains non-timestamped value regardless of rollback; reads at timestamp=2 show non-timestamped value wins. Then swaps the roles (key2 gets timestamped update, key1 gets non-timestamped); re-reads at timestamp=2 and timestamp=5 to confirm non-timestamped updates are always visible.
- **Components:** `txn.c`, `txn_timestamp.c`, `txn_rollback_to_stable.c`, `history_store.c`
- **Notes:** Parameterized over string-row and column key formats. Key insight: non-timestamped writes cover all timestamped history for that key and remain visible regardless of rollback.
