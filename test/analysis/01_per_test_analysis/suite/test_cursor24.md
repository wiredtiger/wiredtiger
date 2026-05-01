# test_cursor24 — Version cursor prepare metadata fields (prepare_ts, durable timestamps)

**File:** `test/suite/test_cursor24.py`
**Storage mode:** General
**Components under test:** version cursor, prepared transactions, prepare/durable timestamps, MVCC

## Test Cases

### `test_cursor24.test_prepare_commit_metadata`
- **What it tests:** Reads prepare metadata via version cursor after a prepared transaction is committed. Verifies `start_prepare_ts`, `stop_prepare_ts`, and `durable_ts` fields are set correctly in the version cursor output.
- **Components:** `src/cursor/cur_version.c`, `src/txn/txn_prepare.c`
- **Notes:** File URI. Scenarios: row and var.

### `test_cursor24.test_prepare_commit_tombstone_metadata`
- **What it tests:** Reads version cursor metadata after a prepared delete (tombstone) is committed; verifies tombstone prepare metadata fields.
- **Components:** `src/cursor/cur_version.c`, `src/txn/txn_prepare.c`

### `test_cursor24.test_prepare_rollback_key_not_found`
- **What it tests:** After a prepared insert is rolled back, the key does not exist and version cursor returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor24.test_prepare_rollback_then_update`
- **What it tests:** After a prepared insert is rolled back, a new committed value is written. Version cursor shows only the committed version (no aborted prepare metadata).
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor24.test_non_prepared_zero_fields`
- **What it tests:** For a non-prepared (regular committed) update, version cursor prepare metadata fields (`start_prepare_ts`, `stop_prepare_ts`) are zero.
- **Components:** `src/cursor/cur_version.c`
