# test_dictionary04 — Dictionary reuse despite combined RLE and time window metadata

**File:** `test/suite/test_dictionary04.py`
**Storage mode:** General
**Components under test:** btree reconciliation, dictionary compression, RLE, time windows, VLCS

## Test Cases

### `test_dictionary04.test_dictionary04`
- **What it tests:** Verifies that value cells reuse dictionary entries correctly when cells simultaneously carry both time window information (commit_timestamp=20) and potential RLE metadata (for VLCS). Inserts two seed values, then commits 7 copies of `value_a` at timestamp 20. After checkpoint, checks `rec_dictionary`:
  - Row-store: 7 cells written individually, each reuses the entry (count = 7).
  - VLCS: RLE compresses the 7 timestamped cells into 1 cell that reuses the entry (count = 1).
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_rec_dict.c`, `src/txn/`
- **Notes:** Combines the scenarios of test_dictionary02 (RLE) and test_dictionary03 (time window) in a single test. Timestamps pinned at `oldest=1`, `stable=1`. Scenarios: `row` and `var`. Tags: `compression`.
