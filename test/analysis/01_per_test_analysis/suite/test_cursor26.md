# test_cursor26 — Regression: aborted prepared value must not hide underlying committed value (WT-17240)

**File:** `test/suite/test_cursor26.py`
**Storage mode:** General
**Components under test:** version cursor, show_prepared_rollback, prepared transaction rollback, on-disk reconciliation, preserve_prepared

## Test Cases

### `test_cursor26.test_aborted_prepared_does_not_hide_underlying_committed`
- **What it tests:** A key has a committed value (ts=10, value=10). A prepared update (value=20, prepare_ts=20) is applied; the page is reconciled to disk while the prepare is active; the prepared update is then rolled back (rollback_ts=30). A version cursor with `show_prepared_rollback=true` must emit **two** rows: (1) the rolled-back prepared entry (start_txn=WT_TXN_ABORTED, value=20) and (2) the surviving committed entry (start_ts=10, value=10). Regression for WT-17240 where the committed value was dropped after reconciliation happened before rollback.
- **Components:** `src/cursor/cur_version.c`, `src/txn/txn_prepare.c`, `src/btree/bt_rec.c`, `src/history/hs_cursor.c`
- **Notes:** In-memory file URI. `conn_config = 'preserve_prepared=true,precise_checkpoint=true'`. Uses `debug=(release_evict_page=true)` session to force reconciliation while prepare is active. `stable_timestamp` is set to 30 to allow clean shutdown with `precise_checkpoint`.
