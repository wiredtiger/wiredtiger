# test_hs02 — History store: truncation with HS entries and timestamped data

**File:** `test/suite/test_hs02.py`
**Storage mode:** General
**Components under test:** history store, truncate, timestamps, btree

## Test Cases

### `test_hs02.test_hs`
- **What it tests:** Creates two tables. Inserts a first batch (nrows/3 = ~3,333 rows) at timestamp 1 with `bigvalue`. Pins oldest/stable to timestamp 1. Updates all nrows to `bigvalue2` at timestamp 100. Uses the second table to pressure eviction and flush the first table's pages out of cache. Truncates the first half of the first table at timestamp 200. Then verifies:
  - At timestamp 1: sees first batch (`bigvalue`, nrows/3 rows).
  - At timestamp 100: sees all nrows with `bigvalue2`.
  - At timestamp 200: sees only the upper half (nrows/2 rows) with `bigvalue2`.

  The test confirms truncation with in-flight HS entries is handled correctly: old versions remain accessible at their timestamps.
- **Components:** `src/history/`, `src/btree/`, `src/cursor/`, `src/txn/`
- **Notes:** Scenarios: key_format ∈ {`S`, `r`}; value_format=S; cache_size=50MB. `check()` helper validates scan counts and values at a given read timestamp.
