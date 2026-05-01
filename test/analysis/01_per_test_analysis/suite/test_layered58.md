# test_layered58 — Cursor forward and backward walk with page deltas

**File:** `test/suite/test_layered58.py`
**Storage mode:** Disagg/Layered
**Components under test:** cur_layered.c, block_disagg, reconciliation (leaf page delta), checkpoint, timestamped reads

## Test Cases

### `test_layered58.test_cursor_walk_with_delta`
- **What it tests:** Verifies that a cursor can walk forward (`next`) and backward (`prev`) correctly over a layered table that has leaf page deltas. Inserts 99 records at timestamp 10 and one record (key "50") at timestamp 20, checkpoints, then updates key "50" to "value2" at timestamp 30 and checkpoints again (producing a delta). After reopening, performs forward and backward scans at timestamp 30 (expects "value2" for key "50") and at timestamp 10 (expects "value" for all keys visible at that timestamp). Verifies exact record counts in each direction.
- **Components:** cur_layered.c (cursor scan with delta pages), block_disagg (leaf delta), reconciliation (`rec_page_delta_leaf`), checkpoint, timestamped reads
- **Notes:** 100 items (range 1–99). The key "50" has a different commit timestamp (20) than the rest (10), which produces a delta at the second checkpoint. Tests both `cursor.next()` and `cursor.prev()` scan paths across a delta page boundary. Disagg-only.
