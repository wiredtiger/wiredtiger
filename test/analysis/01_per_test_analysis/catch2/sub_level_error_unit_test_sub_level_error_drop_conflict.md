# test_sub_level_error_drop_conflict — Sub-level error codes for drop conflicts

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_drop_conflict.cpp`
**Storage mode:** General (also Tiered via dir_store extension on non-Windows)
**Components under test:** `session->drop`, `WT_CONFLICT_BACKUP`, `WT_CONFLICT_DHANDLE`, `WT_CONFLICT_CHECKPOINT_LOCK`, `WT_CONFLICT_SCHEMA_LOCK`, `WT_CONFLICT_TABLE_LOCK`
**Test type:** Unit

## TEST_CASE: "Test WT_CONFLICT_BACKUP and WT_CONFLICT_DHANDLE" [sub_level_error_drop_conflict, sub_level_error]
### SECTION: "Test WT_CONFLICT_BACKUP"
- **What it tests:** Dropping a table while a backup cursor is open returns `EBUSY` with `sub_level_err=WT_CONFLICT_BACKUP` and the message "the table is currently performing backup and cannot be dropped".
- **Components:** `session->drop`, backup cursor, `WT_CONFLICT_BACKUP`
- **Notes:** Backup cursor holds a connection-level lock that blocks drop.

### SECTION: "Test WT_CONFLICT_DHANDLE with simple table"
- **What it tests:** Dropping a simple table while a regular cursor is open on it returns `EBUSY` with `sub_level_err=WT_CONFLICT_DHANDLE` and the message "another thread is currently holding the data handle of the table".
- **Components:** `session->drop`, open cursor, `WT_CONFLICT_DHANDLE`, `__drop_file`
- **Notes:** Exercises the `__drop_file` code path.

### SECTION: "Test WT_CONFLICT_DHANDLE with columns"
- **What it tests:** Same as above but for a table with explicit columns (exercises `__drop_table` code path).
- **Components:** `session->drop`, open cursor, `WT_CONFLICT_DHANDLE`, `__drop_table`
- **Notes:** Columns config: `key_format=S,value_format=S,columns=(col1,col2)`.

### SECTION: "Test WT_CONFLICT_DHANDLE with tiered storage" (non-Windows only)
- **What it tests:** Dropping a tiered-storage table while a cursor is open returns `EBUSY` with `WT_CONFLICT_DHANDLE`.
- **Components:** `session->drop`, tiered storage, `__drop_tiered`, `WT_CONFLICT_DHANDLE`
- **Notes:** Uses `dir_store` extension. Skipped on Windows.

## TEST_CASE: "Test conflicts with checkpoint/schema/table locks" [sub_level_error_drop_conflict]
### SECTION: "Test CONFLICT_CHECKPOINT_LOCK" (non-Windows only)
- **What it tests:** Dropping with `lock_wait=0` while the checkpoint lock is held by another session returns `EBUSY` with `WT_CONFLICT_CHECKPOINT_LOCK`.
- **Components:** `WT_WITH_CHECKPOINT_LOCK`, `WT_CONFLICT_CHECKPOINT_LOCK`
- **Notes:** Windows spinlock re-entrancy prevents this test from working on Windows.

### SECTION: "Test CONFLICT_SCHEMA_LOCK" (non-Windows only)
- **What it tests:** Dropping with `lock_wait=0` while the schema lock is held by another session returns `EBUSY` with `WT_CONFLICT_SCHEMA_LOCK`.
- **Components:** `WT_WITH_SCHEMA_LOCK`, `WT_CONFLICT_SCHEMA_LOCK`
- **Notes:** Skipped on Windows for the same spinlock re-entrancy reason.

### SECTION: "Test CONFLICT_TABLE_LOCK"
- **What it tests:** Dropping with `lock_wait=0` while the table write lock is held by another session returns `EBUSY` with `WT_CONFLICT_TABLE_LOCK`.
- **Components:** `WT_WITH_TABLE_WRITE_LOCK`, `WT_CONFLICT_TABLE_LOCK`
- **Notes:** This section runs on all platforms including Windows.
