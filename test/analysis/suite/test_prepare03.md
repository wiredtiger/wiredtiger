# test_prepare03 — Cursor operations forbidden after prepare_transaction

**File:** `test/suite/test_prepare03.py`
**Storage mode:** General
**Components under test:** prepared transactions, cursor API restrictions

## Test Cases

### `test_prepare03.test_prepare_cursor`
- **What it tests:** Verifies that all cursor operations are prohibited after `prepare_transaction()` and return the correct error; covers insert, next, prev, get_key, get_value, search, update, remove, reserve, reconfigure, and search_near
- **Components:** `txn/txn_prepare.c`, `cursor/cur_std.c`
- **Notes:** Scenarios: file/table × column/integer-row key formats; each operation is expected to raise `WiredTigerError` with a message indicating the cursor operation is not permitted after prepare; the cursor used is the same cursor that performed writes before prepare
