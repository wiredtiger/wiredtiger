# test_sub_level_error_rollback — Sub-level error codes for rollback workflows

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_rollback.cpp`
**Storage mode:** General
**Components under test:** `__wt_evict_app_assist_worker_check`, `__wti_evict_app_assist_worker`, `__txn_modify_block`, `__wt_txn_is_blocking`, `__wt_modify_reconstruct_from_upd_list`
**Test type:** Unit

## TEST_CASE: "Test functions for error handling in rollback workflows" [sub_level_error_rollback, sub_level_error]
### SECTION: "Test WT_CACHE_OVERFLOW in __wti_evict_app_assist_worker - not safe to proceed with eviction"
- **What it tests:** When the eviction server is not running or the cache is under 100%, `__wt_evict_app_assist_worker_check` returns 0 with no sub-level error.
- **Components:** `__wt_evict_app_assist_worker_check`, eviction server state

### SECTION: "Test WT_CACHE_OVERFLOW in __wti_evict_app_assist_worker - conflicting sub-level error codes"
- **What it tests:** When the eviction cache is stuck and the transaction holds the oldest pinned ID, `__wti_evict_app_assist_worker` returns `WT_ROLLBACK` with `WT_OLDEST_FOR_EVICTION` (not `WT_CACHE_OVERFLOW`), demonstrating that `WT_OLDEST_FOR_EVICTION` takes priority.
- **Components:** `__wti_evict_app_assist_worker`, `WT_OLDEST_FOR_EVICTION`, `WT_CACHE_OVERFLOW` interaction

### SECTION: "Test WT_CACHE_OVERFLOW in __wti_evict_app_assist_worker - cache max wait"
- **What it tests:** When cache is nearly full and `cache_max_wait_us=1` (1 microsecond), eviction times out and returns `WT_ROLLBACK` with `sub_level_err=WT_CACHE_OVERFLOW` and message "Cache capacity has overflown".
- **Components:** `__wti_evict_app_assist_worker`, `WT_CACHE_OVERFLOW`, `cache_max_wait_us`

### SECTION: "Test WT_WRITE_CONFLICT in __txn_modify_block"
- **What it tests:** When an update is invisible to the current snapshot, `__txn_modify_block` returns `WT_ROLLBACK` with `sub_level_err=WT_WRITE_CONFLICT` and message "Write conflict between concurrent operations".
- **Components:** `__txn_modify_block`, `WT_WRITE_CONFLICT`, `WT_TXN_HAS_SNAPSHOT`

### SECTION: "Test WT_OLDEST_FOR_EVICTION in __wt_txn_is_blocking - prepared transaction"
- **What it tests:** A prepared transaction is exempt from `WT_OLDEST_FOR_EVICTION`; the function returns 0.
- **Components:** `__wt_txn_is_blocking`, `WT_TXN_PREPARE`

### SECTION: "Test WT_OLDEST_FOR_EVICTION in __wt_txn_is_blocking - rollback can't be handled"
- **What it tests:** Various conditions (no mods, no running txn, operation not timed out) result in 0 return with no sub-level error.
- **Components:** `__wt_txn_is_blocking`

### SECTION: "Test WT_OLDEST_FOR_EVICTION in __wt_txn_is_blocking - transaction ID"
- **What it tests:** When the transaction's pinned ID or ID equals the global oldest ID, `__wt_txn_is_blocking` returns `WT_ROLLBACK` with `sub_level_err=WT_OLDEST_FOR_EVICTION` and message "Transaction has the oldest pinned transaction ID".
- **Components:** `__wt_txn_is_blocking`, `WT_OLDEST_FOR_EVICTION`, `WT_SESSION_TXN_SHARED`

### SECTION: "Test WT_MODIFY_READ_UNCOMMITTED in __wt_modify_reconstruct_from_upd_list - reader with uncommitted isolation"
- **What it tests:** A read-uncommitted reader attempting to reconstruct a record with modify updates returns `WT_ROLLBACK` with `sub_level_err=WT_MODIFY_READ_UNCOMMITTED`. Snapshot and reconciliation readers return 0.
- **Components:** `__wt_modify_reconstruct_from_upd_list`, `WT_MODIFY_READ_UNCOMMITTED`, `WT_ISO_READ_UNCOMMITTED`
