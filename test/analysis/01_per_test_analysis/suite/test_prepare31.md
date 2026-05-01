# test_prepare31 — Checkpoint skipping/writing aborted prepared updates based on stable and rollback timestamps

**File:** `test/suite/test_prepare31.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, rollback, checkpoint, reconciliation stats, time windows

## Test Cases

### `test_prepare31.test_skip_aborted_prepare_update_if_stable_rollback_timestamp`
- **What it tests:** Verifies that checkpoint does not write an aborted prepared update when its `rollback_timestamp` is already stable (i.e., the rollback is visible to all readers); the `rec_time_window_prepared` stat should be false
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Class extends `test_prepare_preserve_prepare_base` which provides `checkpoint_and_verify_stats()`; uses `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`

### `test_prepare31.test_skip_aborted_prepare_update_if_prepare_timestamp_not_stable`
- **What it tests:** Verifies that checkpoint does not write an aborted prepared update when the `prepare_timestamp` itself is not yet stable (the prepared update is entirely in the unstable range and need not be preserved on disk)
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Same base class; stable_ts < prepare_ts at checkpoint time

### `test_prepare31.test_write_prepare_update_if_rollback_timestamp_not_stable`
- **What it tests:** Verifies that checkpoint writes an aborted prepared update as a prepared entry when `prepare_timestamp` is stable but `rollback_timestamp` is not yet stable; the `rec_time_window_prepared` stat should be true, and both `start_txn` and `stop_txn` stats should be set
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** prepare_ts <= stable_ts < rollback_ts; the update must be preserved so recovery can replay it; both the start and stop time-window entries are written as prepared
