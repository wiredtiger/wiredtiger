# test_error_info03 — Session get_last_error() for EBUSY drop conflicts (lock-level detail)

**File:** `test/suite/test_error_info03.py`
**Storage mode:** General
**Components under test:** session API (get_last_error), schema (drop, alter), locking (checkpoint lock, schema lock, table lock, dhandle lock, backup lock)

## Test Cases

### `test_error_info03.test_conflict_checkpoint`
- **What it tests:** While a second thread holds the checkpoint lock and schema lock (via a slow `session.alter()`), the main thread attempts a `drop(lock_wait=0)`. Asserts `get_last_error()` returns `(EBUSY, WT_CONFLICT_CHECKPOINT_LOCK, "another thread is currently holding the checkpoint lock")`.
- **Components:** `src/schema/`, `src/conn/`, `src/session/`
- **Notes:** Uses `timing_stress_for_test=[session_alter_slow,open_index_slow]` to control lock hold duration.

### `test_error_info03.test_conflict_schema`
- **What it tests:** While the checkpoint lock is held, the main thread drops with `lock_wait=0,checkpoint_wait=0` (skipping checkpoint lock check). Asserts the next lock contention is the schema lock: `(EBUSY, WT_CONFLICT_SCHEMA_LOCK, ...)`.
- **Components:** `src/schema/`, `src/session/`

### `test_error_info03.test_conflict_table`
- **What it tests:** Creates a table with an index, then while a second thread holds the table lock (via `__schema_open_index`, which is delayed by `open_index_slow` stress), attempts `drop(lock_wait=0)`. Asserts `(EBUSY, WT_CONFLICT_TABLE_LOCK, "another thread is currently holding the table lock")`.
- **Components:** `src/schema/`, `src/session/`
- **Notes:** The table lock can be held without the schema lock in `__schema_open_index`, which is why the timing stress is placed there.

### `test_error_info03.test_conflict_backup`
- **What it tests:** Opens a `backup:` cursor and then attempts to drop the table. Asserts `(EBUSY, WT_CONFLICT_BACKUP, "the table is currently performing backup and cannot be dropped")`.
- **Components:** `src/backup/`, `src/schema/`, `src/session/`

### `test_error_info03.test_conflict_dhandle`
- **What it tests:** Opens a regular cursor on the table (holding a dhandle reference) and then attempts to drop the table. Asserts `(EBUSY, WT_CONFLICT_DHANDLE, "another thread is currently holding the data handle of the table")`.
- **Components:** `src/schema/`, `src/session/`

### `test_error_info03.test_uncommitted_data`
- **What it tests:** Begins a transaction and updates a key (leaving it open), then tries to drop. Asserts `(EBUSY, WT_UNCOMMITTED_DATA, ...)`.
- **Components:** `src/schema/`, `src/session/`

### `test_error_info03.test_dirty_data`
- **What it tests:** Commits a transaction without checkpointing, waits 1 second, then tries to drop. Asserts `(EBUSY, WT_DIRTY_DATA, ...)`.
- **Components:** `src/schema/`, `src/session/`
