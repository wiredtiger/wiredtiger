# test_checkpoint22 — Write generation correctness after restart with unchanged tree

**File:** `test/suite/test_checkpoint22.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, write generation, crash recovery, btree skip optimization

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that after a database restart, tables whose btrees were not modified are correctly identified by write generation, and the clean-tree optimization (skipping unchanged trees) does not corrupt subsequent checkpoint cursors or data reads.
- **Components:** `src/checkpoint/checkpoint.c`, `src/btree/`, `src/conn/conn_open.c`
- **Notes:** Two tables are created; only one is modified before a second checkpoint. After crash restart, verifies that write generation numbers are correctly assigned so the unmodified table's checkpoint cursor still returns accurate data. Tests the correctness of the `write_gen` mechanism used to detect in-memory vs on-disk state after restart.
