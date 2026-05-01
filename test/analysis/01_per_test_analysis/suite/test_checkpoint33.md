# test_checkpoint33 — Multiple CC+checkpoint cycles shrink deleted table to minimum size

**File:** `test/suite/test_checkpoint33.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, checkpoint, file shrink, compaction

## Test Cases

### `test_checkpoint33.test_checkpoint33`
- **What it tests:** Verifies that running multiple cycles of checkpoint cleanup followed by checkpoint on a table that has had all its data deleted causes the on-disk file to shrink down to the minimum size (12 KB), confirming that CC + checkpoint together reclaim disk space from deleted data.
- **Components:** `src/conn/conn_sweep.c`, `src/checkpoint/`, `src/block/block_compact.c`
- **Notes:** Populates a table with many rows, deletes all of them, then runs several CC+checkpoint cycles. After each cycle checks the file size. Eventually the file should reach the minimum 12 KB. Tests the end-to-end disk space reclamation path via checkpoint cleanup without explicit compaction.
