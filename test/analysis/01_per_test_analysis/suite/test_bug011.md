# test_bug011 — Eviction with more open files than the eviction server walk limit

**File:** `test/suite/test_bug011.py`
**Storage mode:** General
**Components under test:** eviction, dhandle walk, hazard pointers

## Test Cases

### `test_bug011.test_eviction`
- **What it tests:** Verifies that the eviction server copes correctly when the number of open trees exceeds the internal hazard-pointer limit of 1000. Creates 2000 tables (`SimpleDataSet`, 10 000 rows each, 1 KB allocation/leaf pages), force-evicts everything by reopening the connection, then opens a cursor on every table to pin them in cache. Finally performs 10 000 random search operations across all 2000 tables to exercise the eviction walk path. The test passes if no crash, hang, or assertion failure occurs.
- **Components:** `src/eviction/eviction_walk.c`, `src/session/session_dhandle.c`
- **Notes:** Decorated with `@wttest.longtest` — only runs in long-test mode. Non-parametrized. 1 GB cache configured.
