# test_prepare36 — History store content verification after committed prepared transaction on disk

**File:** `test/suite/test_prepare36.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true,statistics=(all)`)
**Components under test:** prepared transactions, history store, checkpoint, on-disk verification, cell packing

## Test Cases

### `test_prepare36.test_hs_commit_prepare`
- **What it tests:** Inserts a base value, prepares an update with `preserve_prepared`, advances stable past prepare_ts (so the prepared update is written to disk on checkpoint), then commits the prepared transaction; reopens the database; reads directly from the history store cursor to verify the HS entry has correct `start_ts`, `stop_ts`, and value; also reads data at various timestamps to verify correctness
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `checkpoint/checkpoint.c`, `btree/bt_rec.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true,statistics=(all)'`; skipped for disagg (cell packing issue noted in comment); uses `check_ckpt_hs()` helper to open HS cursor directly and inspect time window cells; uses `verify_read_data()` to confirm visibility at timestamps before prepare, at prepare, and at durable_ts; no scenarios; reopens the DB to confirm on-disk HS content persists correctly
