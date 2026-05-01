# test_reserve — WT_CURSOR.reserve update tests

**File:** `test/suite/test_reserve.py`
**Storage mode:** General
**Components under test:** cursor reserve, transactions, concurrency control

## Test Cases

### `test_reserve.test_reserve`
- **What it tests:** Comprehensive test of `cursor.reserve()` covering: repeated updates to a record, repeated reserve+commit, repeated reserve+rollback, reserve then update+commit, and conflict detection (another transaction cannot update a reserved record until the reserver commits).
- **Components:** `src/cursor/cur_std.c`, `src/txn/`
- **Notes:** Parameterized on key format (integer, recno, string) and table type (file, table-complex, table-index, table-simple). Uses a secondary session to test write-write conflict on reserved keys.

### `test_reserve.test_reserve_without_key`
- **What it tests:** Verifies that `cursor.reserve()` fails with an error message `/requires key be set/` when no key has been set on the cursor.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Parameterized on key format and table type.

### `test_reserve.test_reserve_without_txn`
- **What it tests:** Verifies that `cursor.reserve()` fails with `/only permitted in a running transaction/` when called outside a transaction.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`
- **Notes:** Parameterized on key format and table type.

### `test_reserve.test_reserve_returns_value`
- **What it tests:** Verifies that a successful `cursor.reserve()` call returns (makes available) the current value of the reserved record via `cursor.get_value()`.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Parameterized on key format and table type.

### `test_reserve.test_reserve_not_supported`
- **What it tests:** Verifies that `cursor.reserve()` returns `WT_NOTSUPP` on bulk and dump cursors (non-standard cursors), and on special cursor types: backup, config, log, metadata, statistics.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Under disagg hook, bulk cursor is excluded. Other special cursor URIs tested as a flat list.
