# test_cc06 — Checkpoint cleanup ignores empty and newly created files

**File:** `test/suite/test_cc06.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, statistics

## Test Cases

### `test_cc06.test_cc`
- **What it tests:** Verifies that CC reports zero `checkpoint_cleanup_pages_visited` for a table that was just created and populated but has no obsolete time window information — both before and after a connection reopen.
- **Components:** `src/btree/`, `src/conn/conn_sweep.c`
- **Notes:** Two scenarios: column and integer_row key formats. Table is created with `log=(enabled=false)`. After setting `oldest_timestamp=stable_timestamp=10`, calls `wait_for_cc_to_run()` and asserts `dsrc.checkpoint_cleanup_pages_visited == 0` (per-table stat). Then calls `reopen_conn()` and repeats the check. This tests the "skip newly created / empty files" fast path in CC to avoid unnecessary work.
