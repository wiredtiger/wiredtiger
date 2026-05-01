# test_prepare45 — Disaggregated storage: prepared transaction replication via follower

**File:** `test/suite/test_prepare45.py`
**Storage mode:** Disaggregated/layered only (`disagg_only`)
**Components under test:** prepared transactions, preserve_prepared, layered storage, leader/follower replication, prepared_discover

## Test Cases

### `test_prepare45.test_prepare_insert`
- **What it tests:** Leader inserts keys and prepares the transaction with prepared_id; leader checkpoints (advancing stable past prepare_ts so the prepared update is durable); follower picks up the checkpoint and replays the prepare, then resolves it (commit); step-up follower and verify inserted data is visible
- **Components:** `txn/txn_prepare.c`, `conn/conn_layered_ingest.c`, `cursor/cur_layered.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; uses layered: URI; three-phase pattern: leader → follower → verify

### `test_prepare45.test_prepare_update`
- **What it tests:** Same three-phase pattern as test_prepare_insert but the prepared transaction is an update to an existing key rather than a new insert
- **Components:** `txn/txn_prepare.c`, `conn/conn_layered_ingest.c`, `cursor/cur_layered.c`

### `test_prepare45.test_prepare_delete`
- **What it tests:** Same pattern but the prepared transaction is a delete (tombstone); follower resolves by committing; step-up and verify key is not found
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `conn/conn_layered_ingest.c`, `cursor/cur_layered.c`

### `test_prepare45.test_prepare_delete_between_values`
- **What it tests:** Inserts value_a at ts=10, then prepared delete at ts=20, then value_b at ts=30; follower replays; verifies WT_NOTFOUND at ts=25 and value_b at ts=35
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `conn/conn_layered_ingest.c`

### `test_prepare45.test_prepare_multiple_updates_same_key`
- **What it tests:** Multiple updates on the same key within a single prepared transaction; follower resolves and verifies the final value
- **Components:** `txn/txn_prepare.c`, `conn/conn_layered_ingest.c`

### `test_prepare45.test_prepare_not_captured_insert`
- **What it tests:** Leader prepares an insert but the prepare_ts > stable_ts at checkpoint time, so the prepared update is NOT written to disk (not captured); follower does not see the prepare; step-up and verify key is not found
- **Components:** `txn/txn_prepare.c`, `conn/conn_layered_ingest.c`, `checkpoint/checkpoint.c`
- **Notes:** "not_captured" variants verify that prepare durability correctly requires prepare_ts <= stable_ts at checkpoint time

### `test_prepare45.test_prepare_not_captured_update`
- **What it tests:** Same as test_prepare_not_captured_insert but for an update on an existing key; original value should still be visible after step-up
- **Components:** `txn/txn_prepare.c`, `conn/conn_layered_ingest.c`

### `test_prepare45.test_prepare_not_captured_delete`
- **What it tests:** Same as test_prepare_not_captured_insert but for a delete; original value should still be visible after step-up
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `conn/conn_layered_ingest.c`
