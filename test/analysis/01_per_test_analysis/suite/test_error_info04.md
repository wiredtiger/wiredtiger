# test_error_info04 — get_last_error() not overwritten when commit/rollback triggers app-thread eviction

**File:** `test/suite/test_error_info04.py`
**Storage mode:** General
**Components under test:** session API (get_last_error), eviction, transaction commit/rollback

## Test Cases

### `test_error_info04.test_commit_transaction_skip_save`
- **What it tests:** Starts 100 separate sessions each with a large (500 KB) insert inside an open transaction. Sets `cache_max_wait_ms=2` to encourage application-thread eviction, then commits all sessions. Asserts that after each `commit_transaction()` call, `get_last_error()` still shows `(0, WT_NONE, ...)` — i.e., the commit's own success status is not overwritten by any internal eviction error that occurs during the commit.
- **Components:** `src/txn/`, `src/evict/`, `src/session/`
- **Notes:** `conn_config` is `cache_max_wait_ms=1,eviction_dirty_target=1,eviction_dirty_trigger=2`. Verifies that internal eviction side-effects during commit do not pollute the session's last-error state.

### `test_error_info04.test_rollback_transaction_skip_save`
- **What it tests:** Same setup as above but calls `rollback_transaction()` instead of `commit_transaction()`. Asserts that after each rollback, `get_last_error()` still returns `(0, WT_NONE, ...)`.
- **Components:** `src/txn/`, `src/evict/`, `src/session/`
- **Notes:** Ensures that rollback (which also may trigger app-thread eviction) does not save any internal eviction error into the session's last-error slot.
