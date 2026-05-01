# test_prepare_hs02 — History store with prepared updates: four scenarios of insert/update/remove with checkpoint

**File:** `test/suite/test_prepare_hs02.py`
**Storage mode:** General
**Components under test:** prepared transactions, history store, checkpoint, insert/update/remove, commit/rollback

## Test Cases

### `test_prepare_hs02.test_prepare_conflict`
- **What it tests:** Runs four distinct prepared-transaction scenarios followed by checkpoint, then commits or rolls back: (1) fresh insert+prepare; (2) update existing key + update new key in same prepared txn; (3) remove existing key + update + remove new key in same prepared txn; (4) reopen DB + update multiple keys in a prepared txn; each scenario verifies correct visibility at relevant timestamps after resolution
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `checkpoint/checkpoint.c`, `btree/bt_delete.c`
- **Notes:** Scenarios: column/integer-row × commit/rollback; the "conflict" in the name refers to the general prepare conflict test framework; history store entries are created by the checkpoint taken while the transaction is still prepared; verifies that checkpoint + commit/rollback correctly resolves HS entries and does not leave stale prepared state
