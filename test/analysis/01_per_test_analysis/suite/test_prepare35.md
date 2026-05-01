# test_prepare35 — Two sequential prepared transactions on same key: first rolled back, second written to checkpoint

**File:** `test/suite/test_prepare35.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, sequential prepares, checkpoint, reconciliation stats, prepared_id

## Test Cases

### `test_prepare35.test_committed_prepare`
- **What it tests:** Issues two prepared transactions on the same key: the first is rolled back (with a rollback_timestamp), the second is committed; verifies that checkpoint correctly writes the second prepared transaction to disk (with a different prepared_id) while the first's rollback tombstone is handled according to the stable_timestamp
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Class extends `test_prepare_preserve_prepare_base`; `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; the two prepares have distinct `prepared_id` values (e.g., id=1 and id=2) so they can be independently tracked; the update chain on disk must correctly associate each time window with the right prepared_id; guards against id collision or incorrect ordering of two sequential prepared updates on the same key
