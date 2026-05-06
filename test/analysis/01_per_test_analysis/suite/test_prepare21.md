# test_prepare21 — Prepare rollback with concurrent checkpoint does not crash

**File:** `test/suite/test_prepare21.py`
**Storage mode:** General
**Components under test:** prepared transactions, rollback, checkpoint, eviction, timing stress

## Test Cases

### `test_prepare21.test_prepare_rollback`
- **What it tests:** Verifies that rolling back a prepared transaction while a concurrent checkpoint thread is running does not cause a crash or assertion failure; guards against an out-of-order fix in the rollback path
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `checkpoint/checkpoint.c`, `evict/evict_page.c`
- **Notes:** Uses `history_store_checkpoint_delay` timing stress failpoint to synchronize the checkpoint thread with eviction during rollback; the concurrent checkpoint runs in a background thread while the main thread performs prepare+evict+rollback; the bug was that rollback could race with checkpoint's traversal of the HS update chain, causing an out-of-order access; no scenarios
