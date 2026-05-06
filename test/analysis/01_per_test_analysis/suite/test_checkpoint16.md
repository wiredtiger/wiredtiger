# test_checkpoint16 — Unmodified (clean) table is readable from checkpoint

**File:** `test/suite/test_checkpoint16.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, clean-table optimization, read path

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a table that was not modified between two checkpoints is still fully readable from the second checkpoint even though the checkpoint skipped reconciling it (clean-table optimization). The checkpoint cursor must see all the data written before the first checkpoint.
- **Components:** `src/checkpoint/checkpoint.c`, `src/cursor/cur_btree.c`, `src/btree/`
- **Notes:** Inserts data, checkpoints, verifies the clean-table path does not skip the table for the checkpoint cursor. The second checkpoint may skip the unmodified table, but opening a checkpoint cursor on it must still correctly locate and return data. Tests that the `WT_BTREE_SKIP_CKPT` optimization does not break cursor reads.
