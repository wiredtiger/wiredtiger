# test_prepare33 — Checkpoint behavior for rolled-back prepared tombstones

**File:** `test/suite/test_prepare33.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, tombstones, rollback, checkpoint, reconciliation stats, time windows

## Test Cases

### `test_prepare33.test_rollback_prepare`
- **What it tests:** Tests checkpoint output for a rolled-back prepared transaction that included a tombstone (remove); at three stable timestamp positions: (1) prepare_ts not stable → no write; (2) prepare_ts stable, rollback_ts not stable → write as prepared (rec_time_window_prepared=true, start_txn=true, stop_txn=true); (3) rollback_ts stable → write only start_ts/start_txn (stop is dropped because the tombstone's rollback is now stable and need not be preserved)
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Class extends `test_prepare_preserve_prepare_base`; `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; the prepared transaction uses `rollback_transaction("rollback_timestamp=...")` with an explicit rollback_timestamp; the stop time window (tombstone) is written as prepared when rollback_ts is unstable, then dropped once rollback_ts is stable
