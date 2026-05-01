# test_hazard — Hazard pointer array growth under many concurrent cursor pins

**File:** `test/suite/test_hazard.py`
**Storage mode:** General
**Components under test:** hazard pointers, btree (page pinning), cursor

## Test Cases

### `test_hazard.test_hazard`
- **What it tests:** Populates a table with 1,000 rows, then opens 10,000 cursors in the same session and positions each on the same key (key 10) to set a hazard pointer. This forces the session's hazard pointer array to grow repeatedly (each new cursor pin may require an array resize). After all cursors are opened, closes them all to clear all hazard pointers. The test verifies that the array growth mechanism works without crashing or corruption.
- **Components:** `src/include/hazard.h`, `src/session/`, `src/btree/`
- **Notes:** Uses `SimpleDataSet` with 1,000 rows. All 10,000 cursors pin the same page (key 10). This is a regression/stress test for the dynamic hazard pointer array.
