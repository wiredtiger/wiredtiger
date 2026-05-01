# test_prepare34 — Checkpoint correctness for prepared modify operations

**File:** `test/suite/test_prepare34.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, modify, checkpoint, reconciliation stats, value reconstruction

## Test Cases

### `test_prepare34.test_rollback_prepare_modify`
- **What it tests:** Inserts a base value, then prepares a `wiredtiger.Modify` operation on that value, rolls back the prepare; verifies that checkpoint correctly handles the aborted prepared modify (writes as prepared when in the unstable window, then resolves once rollback_ts is stable); verifies that value reconstruction at various timestamps returns the correct results
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `modify/modify.c`, `checkpoint/checkpoint.c`
- **Notes:** Class extends `test_prepare_preserve_prepare_base`; `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; modify operations create delta updates in the update chain; aborted modify must be dropped without corrupting the base value

### `test_prepare34.test_commit_prepare_modify`
- **What it tests:** Same as above but commits the prepared modify; verifies checkpoint writes the modify as prepared while durable_ts is unstable, then as committed once durable_ts is stable; verifies the final value (base + delta from modify) is readable at the correct timestamp
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `modify/modify.c`, `checkpoint/checkpoint.c`
- **Notes:** Companion to test_rollback_prepare_modify; exercises the committed path for prepared modifies through the checkpoint machinery
