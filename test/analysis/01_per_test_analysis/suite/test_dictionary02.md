# test_dictionary02 — Dictionary reuse despite RLE information (row-store vs VLCS)

**File:** `test/suite/test_dictionary02.py`
**Storage mode:** General
**Components under test:** btree reconciliation, dictionary compression, RLE, VLCS

## Test Cases

### `test_dictionary02.test_dictionary02`
- **What it tests:** Verifies that value cells that share a value with an existing dictionary entry are correctly reused even when they also carry RLE (Run Length Encoding) metadata (relevant for VLCS/column-store). Inserts two distinct values as dictionary seeds, then inserts 7 more copies of `value_a`. After checkpoint, checks the `rec_dictionary` count:
  - Row-store: 7 cells are individually written and each reuses the entry (count = 7).
  - VLCS: RLE compresses the 7 cells into 1 cell that reuses the entry (count = 1).
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_rec_dict.c`
- **Notes:** Timestamps are pinned (`oldest_timestamp=1`, `stable_timestamp=1`) to prevent global visibility from allowing further reconciliation compression. Scenarios: `row` and `var`. Tags: `compression`.
