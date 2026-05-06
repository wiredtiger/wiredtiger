# test_checkpoint24 — Fast-delete pages in checkpoint (no timestamps)

**File:** `test/suite/test_checkpoint24.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, fast delete, WT_REF_DELETED state, non-timestamped

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that fast-deleted (truncated) pages captured in a checkpoint are correctly visible as deleted when reading from a checkpoint cursor, using non-timestamped transactions.
- **Components:** `src/btree/bt_delete.c`, `src/checkpoint/`, `src/cursor/cur_btree.c`
- **Notes:** Populates a table, truncates a range (creating fast-delete pages in `WT_REF_DELETED` state), then checkpoints. A checkpoint cursor must see the truncated range as empty (no keys). Tests that fast-delete page state is correctly preserved across checkpoint/cursor-open cycles without timestamps.
