# test_checkpoint25 — Fast-delete pages in checkpoint (with timestamps)

**File:** `test/suite/test_checkpoint25.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, fast delete, WT_REF_DELETED state, timestamped transactions

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Timestamped version of test_checkpoint24. Verifies that fast-deleted (truncated) pages with timestamps are correctly visible at the right read timestamp from a checkpoint cursor.
- **Components:** `src/btree/bt_delete.c`, `src/checkpoint/`, `src/cursor/cur_btree.c`, `src/txn/txn_timestamp.c`
- **Notes:** Truncation committed at a specific timestamp. Checkpoint cursor at `read_timestamp` before the truncation commit sees the rows; at `read_timestamp` after sees them as deleted. Tests MVCC visibility of fast-delete pages across the checkpoint cursor read path.
