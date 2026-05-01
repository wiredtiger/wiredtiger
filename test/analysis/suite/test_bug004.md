# test_bug004 — Overflow keys/values readable from snapshot after truncate-and-checkpoint

**File:** `test/suite/test_bug004.py`
**Storage mode:** General
**Components under test:** overflow keys/values, reconciliation, truncate, MVCC snapshot

## Test Cases

### `test_bug004.test_bug004`
- **What it tests:** Creates a file with very small pages (leaf_page_max=512) and inserts 30 records with overflow-sized keys and values. Verifies the file, forces it to disk (reopen_conn), verifies again. Starts a transaction in a separate session (to pin a snapshot), then uses `session.truncate()` to remove a range of records without instantiating keys (the truncate path does not instantiate overflow keys). Checkpoints (freeing the overflow blocks). Uses the snapshot cursor to read the old overflow key/value pairs, verifying they are still accessible via MVCC.
- **Components:** `src/btree/bt_ovfl.c`, `src/btree/bt_delete.c`, `src/reconciliation/rec_write.c`, `src/txn/txn_api.c`
- **Notes:** Parametrized across `column` (r) and `row_string` (S) key formats. The row key is a repetition of the simple_key, making it overflow-length. The value is also overflow-sized (`abcdef * 100`).
