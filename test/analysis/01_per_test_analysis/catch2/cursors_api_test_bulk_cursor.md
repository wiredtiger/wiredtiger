# test_bulk_cursor — Bulk cursor / non-bulk cursor behavior under checkpoint and drop

**File:** `test/catch2/cursors/api/test_bulk_cursor.cpp`
**Storage mode:** General
**Components under test:** Bulk cursor API, checkpoint, drop, transaction lifecycle
**Test type:** API contract

## TEST_CASE: "Cursor: bulk, non-bulk, checkpoint and drop combinations" [cursor]

This test case calls several helper functions, each of which creates its own SECTION internally.

### Helper: cache_destroy_memory_check (config="")
- **What it tests:** Memory is freed correctly when using a non-bulk cursor through an insert-commit cycle.
- **Components:** Cache statistics, session, cursor, transaction commit
- **Notes:** Checks `pages_inmem`, `pages_evicted`, and dirty bytes before and after commit.

### Helper: cache_destroy_memory_check (config="bulk")
- **What it tests:** Opening a bulk cursor during an active transaction returns EINVAL.
- **Components:** Bulk cursor, transaction state validation
- **Notes:** Bulk cursors require no active transaction at open time.

### Helper: cursor_test (config="", close=false)
#### SECTION: "Checkpoint during transaction then commit"
- **What it tests:** Calling checkpoint during an active (non-bulk) transaction returns EINVAL.
- **Components:** `session->checkpoint`, active transaction guard
- **Notes:** `expected_commit_result = EINVAL` (transaction tainted by checkpoint attempt).

#### SECTION: "Checkpoint in 2nd thread during transaction then commit"
- **What it tests:** A checkpoint initiated from a second thread during an active transaction is handled safely.
- **Components:** Threading, checkpoint, transaction commit
- **Notes:** Commit returns EINVAL after cross-thread checkpoint attempt.

#### SECTION: "Drop in 2nd thread during transaction then commit"
- **What it tests:** A force-drop from another thread while a cursor is open commits with EINVAL.
- **Components:** Threading, force drop, transaction commit
- **Notes:** Ensures the drop does not corrupt transaction state.

#### SECTION: "Checkpoint in 2nd thread during transaction then rollback"
- **What it tests:** Rolling back a transaction after a cross-thread checkpoint succeeds (returns 0).
- **Components:** Threading, checkpoint, transaction rollback
- **Notes:** Rollback is always safe regardless of checkpoint activity.

#### SECTION: "Drop then checkpoint in one thread (cursor not closed)"
- **What it tests:** Dropping the table while the cursor is still open returns EBUSY; subsequent checkpoint returns EINVAL.
- **Components:** Drop, open cursor conflict
- **Notes:** The cursor holds the data handle; drop is blocked.

### Helper: cursor_test (config="bulk", close=false/true)
- **What it tests:** Bulk cursors cannot be opened during an active transaction (EINVAL). After the transaction ends, expected commit succeeds.
- **Components:** Bulk cursor, transaction, checkpoint, drop
- **Notes:** Various section combinations with `close=true/false`.

### Helper: multiple_drop_test (config="bulk", ...)
- **What it tests:** Repeated create/open-bulk-cursor/drop/checkpoint/commit cycles work correctly across 5 iterations.
- **Components:** Bulk cursor lifecycle, repeated drop, checkpoint, commit
- **Notes:** Tests with `do_sleep=false` and `do_sleep=true` variants. Confirms the loop runs exactly 5 times.
