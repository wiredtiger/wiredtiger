# test_rollback_to_stable29 — RTS history store ordering with non-timestamp update inserted to tombstone

**File:** `test/suite/test_rollback_to_stable29.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, non-timestamp updates, tombstone, crash recovery

## Test Cases

### `test_rollback_to_stable29.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly handles HS ordering when a non-timestamp update is inserted into a key that was previously tombstoned. Writes value_a@10, pins stable=10. Opens a long-running reader. Removes@30, inserts value_b@40 (evicts), inserts value_c@50 (evicts). Then inserts value_d without a timestamp (`ts=0`). Checkpoints and crash-restarts. Post-crash: value_d is visible (it's a global/non-timestamp update that survives RTS). `hs_removed >= 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/log/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer). `cache_size=5MB`, `log=(enabled=true)`. Long-running reader session (pinned at ts=10) is opened to keep older versions in HS. No eviction of the non-timestamp value before crash. The test focuses on the ordering invariant: non-timestamp update must appear correctly in HS order after remove+timestamp updates.
