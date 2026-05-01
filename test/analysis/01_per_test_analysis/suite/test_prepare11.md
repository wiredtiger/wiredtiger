# test_prepare11 — Reserved update sandwiched between prepared updates resolves correctly

**File:** `test/suite/test_prepare11.py`
**Storage mode:** General
**Components under test:** prepared transactions, cursor reserve, resolve_prepared_op, update chain

## Test Cases

### `test_prepare11.test_prepare_update_rollback`
- **What it tests:** Inserts an initial value, then in a single prepared transaction: performs an update (value_b), reserves the key (cursor.reserve()), and performs another update (value_c); commits or rolls back the prepared transaction; verifies that the resolve_prepared_op handles all three prepared updates (update, reserve, update) in the chain correctly
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `cursor/cur_std.c`, `btree/bt_update.c`
- **Notes:** Scenarios: column/integer-row × commit/rollback; the reserved update creates a placeholder in the update chain that must be resolved along with the surrounding updates when the prepare is resolved; guards against a bug where only the first or last prepared update in the chain was resolved, leaving stale entries
