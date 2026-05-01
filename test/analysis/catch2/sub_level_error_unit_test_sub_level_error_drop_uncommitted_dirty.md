# test_sub_level_error_drop_uncommitted_dirty — Sub-level errors for uncommitted/dirty data drop conflicts

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_drop_uncommitted_dirty.cpp`
**Storage mode:** General
**Components under test:** `__wt_conn_dhandle_close`, `WT_UNCOMMITTED_DATA`, `WT_DIRTY_DATA`
**Test type:** Unit

## TEST_CASE: "Test WT_UNCOMMITTED_DATA and WT_DIRTY_DATA" [sub_level_error_drop_uncommitted_dirty, sub_level_error]
### SECTION: "Test WT_UNCOMMITTED_DATA is not thrown"
- **What it tests:** Closing a dhandle without the visibility check (`visibility_only=false`) does not raise `WT_UNCOMMITTED_DATA`.
- **Components:** `__wt_conn_dhandle_close`

### SECTION: "Test WT_UNCOMMITTED_DATA is not thrown (with visibility check only)"
- **What it tests:** Closing with visibility check enabled but no uncommitted transaction does not raise an error.
- **Components:** `__wt_conn_dhandle_close`, visibility check

### SECTION: "Test WT_UNCOMMITTED_DATA is not thrown (with uncommitted txn only)"
- **What it tests:** Having a high `max_upd_txn` without the visibility check does not trigger the error.
- **Components:** `__wt_conn_dhandle_close`, `max_upd_txn`

### SECTION: "Test WT_UNCOMMITTED_DATA is thrown (with both visibility check and uncommitted txn)"
- **What it tests:** When `max_upd_txn=100` (simulating an uncommitted transaction that can never reach oldest) and `visibility_only=true`, closing returns `EBUSY` with `sub_level_err=WT_UNCOMMITTED_DATA`.
- **Components:** `__wt_conn_dhandle_close`, `WT_UNCOMMITTED_DATA`
- **Notes:** Both conditions must be present simultaneously.

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is unmodified, is not bulk, is not metadata)"
- **What it tests:** A clean, non-bulk, non-metadata btree closes without raising `WT_DIRTY_DATA`.
- **Components:** `__wt_conn_dhandle_close`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is unmodified, is not bulk, is metadata)"
- **What it tests:** A metadata btree does not raise `WT_DIRTY_DATA` even when dirty, because metadata is always exempt.
- **Components:** `__wt_conn_dhandle_close`, `WT_DHANDLE_IS_METADATA`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is unmodified, is bulk, is not metadata)"
- **What it tests:** A bulk btree does not raise `WT_DIRTY_DATA` because bulk loads are exempt.
- **Components:** `__wt_conn_dhandle_close`, `WT_BTREE_BULK`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is unmodified, is bulk, is metadata)"
- **What it tests:** Both bulk and metadata exemptions apply simultaneously.
- **Components:** `__wt_conn_dhandle_close`

### SECTION: "Test WT_DIRTY_DATA is thrown (btree is modified, is not bulk, is not metadata)"
- **What it tests:** A modified non-bulk non-metadata btree returns `EBUSY` with `sub_level_err=WT_DIRTY_DATA` and the message "the table has dirty data and can not be dropped yet".
- **Components:** `__wt_conn_dhandle_close`, `WT_DIRTY_DATA`, `btree->modified`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is modified, is not bulk, is metadata)"
- **What it tests:** A modified metadata btree is exempt from `WT_DIRTY_DATA`.
- **Components:** `__wt_conn_dhandle_close`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is modified, is bulk, is not metadata)"
- **What it tests:** A modified bulk btree is exempt from `WT_DIRTY_DATA`.
- **Components:** `__wt_conn_dhandle_close`

### SECTION: "Test WT_DIRTY_DATA is not thrown (btree is modified, is bulk, is metadata)"
- **What it tests:** Modified + bulk + metadata is exempt from `WT_DIRTY_DATA`.
- **Components:** `__wt_conn_dhandle_close`
