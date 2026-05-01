# test_hs26 — History store: variable-length column store RLE groups with overlapping timestamp ranges

**File:** `test/suite/test_hs26.py`
**Storage mode:** General (column-store only; no row-store scenarios)
**Components under test:** history store, variable-length column store, RLE encoding, eviction

## Test Cases

### `test_hs26.test_hs`
- **What it tests:** Writes batches of values with duplicate-value suffixes (to create RLE-encodable groups of different sizes: 7, 13, or 17 keys per group) at timestamp_1=2. Optionally makes this data globally visible. Creates a long-running reader at ts=2. Writes a second set of values at timestamp_2=100 with different groupings (different modulus). Forces eviction on every 41st key (to trigger RLE-encoding and re-reading). Verifies the long-running reader still sees all timestamp_1 values, and the main session sees timestamp_2 values.

  Tests multiple scenarios of:
  - Whether timestamp_1 data is globally visible before timestamp_2 writes
  - Different numbers of rows for first/second write (more, same, less)
  - Different RLE group sizes (7, 13, 17 for each write)
- **Components:** `src/history/`, `src/column/`, `src/evict/`
- **Notes:** 108 scenarios (2 × 3 × 3 × 3). Key insight: when RLE groups from two different timestamp ranges overlap (e.g., rows 1–7 at one timestamp and rows 5–17 at another), eviction and HS reconstruction must correctly handle split and merged RLE groups. Uses `SimpleDataSet` with `key_format='r'`.
