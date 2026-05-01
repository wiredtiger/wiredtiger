# test_prepare32 — Checkpoint write behavior at different stable timestamps for committed prepared updates

**File:** `test/suite/test_prepare32.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, checkpoint, reconciliation stats, time windows, durable timestamp

## Test Cases

### `test_prepare32.test_committed_prepare`
- **What it tests:** Tests checkpoint output at four distinct stable timestamp positions relative to a committed prepared transaction: (1) prepare_ts not stable → no write; (2) prepare_ts stable but commit (durable) ts not → write as prepared (rec_time_window_prepared=true); (3) durable_ts stable → write as committed (rec_time_window_durable_start_ts, rec_time_window_start_ts, rec_time_window_start_txn=true, prepared=false); (4) no change → page clean, no write
- **Components:** `txn/txn_prepare.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** Class extends `test_prepare_preserve_prepare_base`; `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; verifies the complete lifecycle of how a committed prepared update is written to disk across successive checkpoints as stable_ts advances
