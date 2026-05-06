# test_dictionary01 — Smoke test for dictionary compression effectiveness

**File:** `test/suite/test_dictionary01.py`
**Storage mode:** General
**Components under test:** btree reconciliation, dictionary compression, statistics

## Test Cases

### `test_dictionary01.test_dictionary01`
- **What it tests:** Inserts 25,000 alternating key-value pairs (two repeating values) into a file with `dictionary=100` and a 64K leaf page. After checkpointing to force reconciliation, verifies via the `rec_dictionary` statistic that the dictionary compression eliminated at least `nentries - 100` duplicate value cells.
- **Components:** `src/btree/bt_rec.c`, `src/btree/bt_rec_dict.c`, `src/support/stat.c`
- **Notes:** Scenarios: `row` (key_format=S) and `var` (key_format=r / VLCS). Alternating values are used specifically to prevent column-store RLE from compressing them into a single cell, isolating the dictionary compression effect. Tags: `compression`.
