# test_cursor25 — show_prepared_rollback version cursor option (in-memory only)

**File:** `test/suite/test_cursor25.py`
**Storage mode:** General
**Components under test:** version cursor, show_prepared_rollback, prepared transaction rollback, in-memory tables

## Test Cases

### `test_cursor25.test_rollback_insert_only`
- **What it tests:** A prepared insert is rolled back; version cursor with `show_prepared_rollback=true` emits the aborted update entry (identified by `start_txn == WT_TXN_ABORTED`).
- **Components:** `src/cursor/cur_version.c`, `src/txn/`
- **Notes:** In-memory file URI only. Scenarios: row and var.

### `test_cursor25.test_rollback_insert_then_committed_insert`
- **What it tests:** A committed insert followed by a prepared insert that is rolled back; version cursor emits both the aborted entry and the committed entry.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor25.test_rollback_update_over_committed`
- **What it tests:** A prepared update over a committed value is rolled back; version cursor emits the aborted update and the surviving committed value.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor25.test_rollback_delete_over_committed`
- **What it tests:** A prepared delete over a committed value is rolled back; version cursor emits the aborted tombstone and the surviving committed value.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor25.test_rollback_insert_then_committed_update`
- **What it tests:** A committed insert, then a committed update, then a rolled-back prepared update; version cursor emits aborted entry plus both committed versions.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor25.test_rollback_insert_delete_same_txn`
- **What it tests:** A prepared transaction that inserts and then deletes the same key is rolled back; version cursor emits the aborted entries.
- **Components:** `src/cursor/cur_version.c`, `src/txn/`

### `test_cursor25.test_rollback_insert_flag_without_visible_only`
- **What it tests:** `show_prepared_rollback=true` without `visible_only=true`; verifies both visible and invisible versions are returned including aborted entries.
- **Components:** `src/cursor/cur_version.c`

### `test_cursor25.test_show_prepared_rollback_requires_in_memory`
- **What it tests:** Opening a version cursor with `show_prepared_rollback=true` on a non-in-memory table; expects an error (feature requires `in_memory=true`).
- **Components:** `src/cursor/cur_version.c`
- **Notes:** Verifies the constraint that `show_prepared_rollback` only works on in-memory trees.
