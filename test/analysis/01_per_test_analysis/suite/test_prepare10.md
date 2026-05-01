# test_prepare10 — Prepare rollback correctly restores time windows after removes

**File:** `test/suite/test_prepare10.py`
**Storage mode:** General
**Components under test:** prepared transactions, rollback, time window management, snapshot visibility

## Test Cases

### `test_prepare10.test_prepare_rollback_retrieve_time_window`
- **What it tests:** Executes a sequence of removes on a key followed by a prepared update on the same key; rolls back the prepared update; verifies that the time windows on the update chain are correctly restored to their pre-prepare state and that concurrent sessions maintain correct snapshot visibility
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_update.c`, `cell/cell_inline.h`
- **Notes:** No scenarios; tests the internal time window (start_ts, stop_ts, prepare_ts) metadata is correctly reverted when a prepare is aborted; a second session with an open snapshot verifies it still sees the correct data at its read timestamp; guards against a bug where rollback incorrectly updated time window metadata
