# test_prepare39 — History store content verification after rolled-back prepared transaction on disk

**File:** `test/suite/test_prepare39.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true,statistics=(all)`)
**Components under test:** prepared transactions, history store, rollback, checkpoint, on-disk verification

## Test Cases

### `test_prepare39.test_hs_rollback_prepare`
- **What it tests:** Inserts a base value, prepares an update, advances stable past prepare_ts (so the prepared update is written to disk), checkpoints; then rolls back the prepared transaction with a rollback_timestamp; verifies that before the rollback_ts is stable, the HS entry has `stop_ts=MAX` (representing the uncommitted state); after advancing stable past rollback_ts and recheckpointing, the HS entry has `stop_ts=rollback_ts` (correctly bounded); reopens DB and verifies on-disk HS state persists correctly
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `history/hs_cursor.c`, `checkpoint/checkpoint.c`, `btree/bt_rec.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true,statistics=(all)'`; uses `check_ckpt_hs()` helper to inspect HS time window cells directly; uses `verify_read_data()` for visibility checks; no scenarios; companion to test_prepare36 (which covers the committed path); the MAX stop_ts is a sentinel meaning "stop is still unresolved" for a rolled-back prepare whose rollback_ts is not yet stable
