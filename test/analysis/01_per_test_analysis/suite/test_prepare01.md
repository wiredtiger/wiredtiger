# test_prepare01 — Prepared transaction visibility and read_timestamp interaction

**File:** `test/suite/test_prepare01.py`
**Storage mode:** General
**Components under test:** prepared transactions, transaction visibility, isolation levels, checkpoint

## Test Cases

### `test_prepare01.test_visibility`
- **What it tests:** Inserts 1,000 entries while periodically issuing prepare+commit cycles; verifies that prepared-but-not-yet-committed updates are invisible to snapshot and read-committed transactions, while previously committed data is visible; also verifies checkpoint sees stable data
- **Components:** `txn/txn_prepare.c`, `txn/txn.c`, `btree/bt_cursor.c`
- **Notes:** Does not use scenarios (single configuration); tests visibility from read-uncommitted, snapshot, and read-committed isolation levels before and after commit; takes a checkpoint partway through and verifies checkpoint cursor sees only data stable at checkpoint time

### `test_prepare01_read_ts.test_prepare01_read_ts`
- **What it tests:** Verifies that attempting to set `read_timestamp` after `prepare_transaction()` is silently ignored (or emits a specific stderr message rather than returning an error)
- **Components:** `txn/txn_prepare.c`, `txn/txn_timestamp.c`
- **Notes:** Expects a stderr pattern indicating the read timestamp setting was ignored; separate class with minimal setup; no scenarios
