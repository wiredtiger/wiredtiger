# test_checkpoint29 — Checkpoint cursor after bulk load operations

**File:** `test/suite/test_checkpoint29.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, bulk load, read path

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a checkpoint taken after a bulk-load operation produces a readable checkpoint cursor that returns all bulk-loaded data correctly.
- **Components:** `src/cursor/cur_bulk.c`, `src/checkpoint/`, `src/cursor/cur_btree.c`
- **Notes:** Uses bulk-load cursor to insert a large sorted dataset, then checkpoints and opens a checkpoint cursor. Reads all records and verifies count and values. Tests that the bulk-load code path (which bypasses normal btree insert) correctly writes data that is visible in subsequent checkpoint cursors.
