# test_checkpoint03 — Checkpoint writes data to the history store

**File:** `test/suite/test_checkpoint03.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, history store, statistics

## Test Cases

### `test_checkpoint03.test_checkpoint03`
- **What it tests:** Verifies that performing a checkpoint with concurrent multi-version updates causes data to be written to the history store (`cache_write_hs` stat increments).
- **Components:** `src/checkpoint/`, `src/history/hs_cursor.c`, `src/history/hs_rec.c`
- **Notes:** Multiple update rounds at increasing timestamps with `stable_timestamp` held back force old versions into the HS at checkpoint time. Asserts `stat.conn.cache_write_hs > 0` after checkpoint.
