# test_cursor19 — Version cursor with modify operations (modify chain, on-disk, HS layers)

**File:** `test/suite/test_cursor19.py`
**Storage mode:** General
**Components under test:** version cursor (dump_version), cursor modify, history store, eviction

## Test Cases

### `test_cursor19.test_modify`
- **What it tests:** Inserts a base value, applies multiple `cursor.modify()` operations, forces eviction (creating on-disk and history store layers), then reads via version cursor. Verifies that each modify shows as a separate version entry with type `1` (modify) and the final full value has type `3` (standard). Also tests deletion at the end of the modify chain.
- **Components:** `src/cursor/cur_version.c`, `src/cursor/cur_modify.c`, `src/history/hs_cursor.c`, `src/btree/`
- **Notes:** File URI only. Scenarios: row (`key_format=S`) and var (`key_format=r`). Uses `debug=(release_evict_page=true)` to force reconciliation. Version types: 1=modify, 3=standard (full value), 4=tombstone.
