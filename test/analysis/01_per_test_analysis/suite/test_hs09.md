# test_hs09 — History store: correct data store / HS partitioning during reconciliation

**File:** `test/suite/test_hs09.py`
**Storage mode:** General
**Components under test:** history store, checkpoint, reconciliation, prepared transactions

## Test Cases

### `test_hs09.test_uncommitted_updates_not_written_to_hs`
- **What it tests:** Writes committed versions at ts=2 and ts=3, then begins but does not commit a transaction with updates at rows 1–10. Checkpoints. Asserts that the data store checkpoint cursor shows `value2` (ts=3) and the HS cursor shows `value1` (ts=2 to ts=3). Uncommitted `value3` should not appear anywhere.
- **Components:** `src/history/`, `src/checkpoint/`, `src/reconcile/`

### `test_hs09.test_prepared_updates_not_written_to_hs`
- **What it tests:** Writes committed versions at ts=2 and ts=3, prepares updates (ts=4 for rows 1–10) and leaves them prepared. Checkpoints. Asserts that prepared values are not written to HS as committed versions. Checkpoint cursors use `ignore_prepare` so prepared updates may appear in data store.
- **Components:** `src/history/`, `src/txn/`, `src/checkpoint/`

### `test_hs09.test_write_newest_version_to_data_store`
- **What it tests:** Writes two committed versions at ts=2 and ts=3. Checkpoints. Asserts that the data store contains `value2` (newest) and the HS contains `value1` with start_ts=2, stop_ts=3.
- **Components:** `src/history/`, `src/checkpoint/`

### `test_hs09.test_write_deleted_version_to_data_store`
- **What it tests:** Writes committed versions at ts=2 and ts=3, then deletes all records at ts=4. Checkpoints. Asserts the data store contains no values (tombstone visible) and the HS shows `value1` with correct timestamp range. The Python comparison `self.assertEqual(0, ...)` on missing cursor returns catches any unexpected data presence.
- **Components:** `src/history/`, `src/btree/`, `src/checkpoint/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`, `S`}; nrows=1000; cache_size=20MB. Opens raw `file:WiredTigerHS.wt` checkpoint cursor to inspect HS content directly. Checks `type != 5` (no tombstones in HS), `type != 1` (no birthmarks in HS), `type == 4` (WT_UPDATE_STANDARD).
