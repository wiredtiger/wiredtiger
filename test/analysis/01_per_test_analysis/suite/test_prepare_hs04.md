# test_prepare_hs04 — Prepared re-insert on key with tombstone: visibility with ignore_prepare and after crash/RTS

**File:** `test/suite/test_prepare_hs04.py`
**Storage mode:** General (skipped for disagg via decorator)
**Components under test:** prepared transactions, history store, tombstones, ignore_prepare, rollback_to_stable, crash recovery

## Test Cases

### `test_prepare_hs04.test_prepare_hs`
- **What it tests:** Inserts a key at ts=2 (committed); deletes the key at ts=10 (committed); prepares a re-insert of the same key at ts=20; verifies visibility with `ignore_prepare=true/false` at ts=5 and ts=20 (both before and during the prepare window); simulates a crash+RTS via copy to RESTART directory; after recovery verifies: ts=5 → committed value, ts=20 → WT_NOTFOUND (tombstone from ts=10 is the last stable value), ts=20 with ignore_prepare → WT_NOTFOUND; if committed before crash, also verifies ts=30 → prepared value
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `btree/bt_delete.c`, `rts/rts.c`, `conn/conn_recover.c`
- **Notes:** Scenarios: commit/rollback × column/string-row; commit_key must exceed nrows for column format to avoid key collision; `conn_config = 'cache_size=5MB,statistics=(fast)'`; uses `copy_wiredtiger_home` for crash simulation (not simulate_crash_restart); the key test: prepared re-insert on a key with a tombstone in the history store triggers complex HS traversal logic
