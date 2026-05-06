# test_checkpoint13 — Checkpoint cursor API restrictions and error handling

**File:** `test/suite/test_checkpoint13.py`
**Storage mode:** General
**Components under test:** checkpoint cursor API, timestamps, error handling

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies three API restriction scenarios for checkpoint cursors: (1) reads within a transaction using a checkpoint cursor succeed; (2) reading with `read_timestamp` before `oldest_timestamp` raises an error; (3) dropping or regenerating a named checkpoint while a cursor is open on it returns `EBUSY`.
- **Components:** `src/session/session_api.c`, `src/cursor/cur_btree.c`, `src/checkpoint/`
- **Notes:** Tests the documented invariants of checkpoint cursor usage. The `read_timestamp < oldest_timestamp` restriction ensures that reads cannot access data that has been garbage-collected. The in-use drop restriction prevents concurrent modification of checkpoint state while it is being read.
