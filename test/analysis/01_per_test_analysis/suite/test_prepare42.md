# test_prepare42 — Prepared insert rollback and prepared insert on globally-visible tombstone

**File:** `test/suite/test_prepare42.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, insert rollback, globally-visible tombstone, checkpoint, reconciliation stats

## Test Cases

### `test_prepare42.test_prepare_insert_rollback`
- **What it tests:** Prepares a new insert (key did not exist before) and rolls it back; verifies at multiple stable timestamps that checkpoint handles the aborted prepared insert correctly — the key should not be visible after rollback, and the checkpoint does not write any prepared start/stop entries once the rollback is stable
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; inserts a key for the first time (no prior history), prepares it, rolls back with a rollback_timestamp; covers the edge case where the entire prepared update chain has no prior committed value

### `test_prepare42.test_prepare_insert_rollback_with_globally_visible_stop_point`
- **What it tests:** Inserts a key, deletes it (ts=10), advances oldest past the delete (making the tombstone globally visible), then prepares a new insert on the same key (ts=20) and rolls it back; verifies checkpoint behavior when the prepared update sits on top of a globally-visible tombstone (the prior value is entirely gone from the chain)
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_delete.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Tests the complex interaction between a globally-visible stop point (the oldest-advanced tombstone) and a prepared insert rollback on the same key
