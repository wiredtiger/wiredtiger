# test_prepare04 — Prepare conflict and write conflict detection

**File:** `test/suite/test_prepare04.py`
**Storage mode:** General
**Components under test:** prepared transactions, prepare conflict, write conflict, ignore_prepare, timestamps

## Test Cases

### `test_prepare04.test_prepare_conflict`
- **What it tests:** Verifies that a concurrent read at a timestamp after `prepare_timestamp` returns a prepare conflict when `ignore_prepare=false`; verifies that attempting to update a prepared key always returns a write conflict; verifies `ignore_prepare=true` suppresses the conflict and returns the pre-prepared value
- **Components:** `txn/txn_prepare.c`, `txn/txn.c`, `btree/bt_cursor.c`
- **Notes:** Scenarios: column/integer-row × before_ts/after_ts/no_ts (read timestamp relative to prepare_ts) × ignore_prepare true/false; the "before_ts" scenario (read_timestamp before prepare_timestamp) should succeed without conflict; "after_ts" with ignore_prepare=false triggers the conflict; write conflict tested separately for all timestamp positions
