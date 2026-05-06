# test_rollback_to_stable19 — RTS aborts both insert and remove from a single prepared transaction

**File:** `test/suite/test_rollback_to_stable19.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, prepared transactions, crash recovery, clean shutdown, in-memory

## Test Cases

### `test_rollback_to_stable19.test_rollback_to_stable_no_history`
- **What it tests:** Verifies that RTS correctly aborts both the insert and the remove from a single prepared transaction that was not yet committed (no historical values for the keys). A prepared txn inserts+removes 1,000 keys at prepare_ts=20. Page is evicted via `debug=(release_evict)`. Sets stable=20. For on-disk: crash or clean restart; for in-memory: rollback the prepared txn manually. Post-RTS: 0 rows visible at ts=20/30. Stats: for crash mode `upd_aborted > 0` and `keys_removed > 0`; for clean restart/in-memory stats are 0 (RTS ran during shutdown).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`, `src/evict/`
- **Notes:** Parametrized on in_memory, key_format (column/row_integer), crash (true/false). Uses `ignore_prepare=true` for eviction cursor read.

### `test_rollback_to_stable19.test_rollback_to_stable_with_history`
- **What it tests:** Verifies RTS aborts both insert and remove from a prepared txn when history exists. First writes value_a@20, then removes@30, then a prepared txn inserts+removes value_b@40 (prepare_ts=40). Page evicted. Sets stable=40. Crash or clean restart. Post-RTS: value_a visible at ts=20, 0 rows at ts=30/40. Stats: for crash mode `hs_removed > 0` and `upd_aborted > 0`; for clean restart/in-memory both are 0.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`, `src/history/`
- **Notes:** Parametrized on in_memory, key_format, crash. Tests the prepared-txn insert+remove pattern with existing HS.
