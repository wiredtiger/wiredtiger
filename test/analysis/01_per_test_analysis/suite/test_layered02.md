# test_layered02 — Basic layered table cursor open and close

**File:** `test/suite/test_layered02.py`
**Storage mode:** Disagg/Layered
**Components under test:** layered table cursor lifecycle, cur_layered.c

## Test Cases

### `test_layered02.test_layered02`
- **What it tests:** Creates a layered table, opens a cursor on it, then closes the cursor without performing any data operations. Verifies that the cursor open/close lifecycle works without error on a newly created layered table.
- **Components:** cursor management (`cur_layered.c`), layered table schema
- **Notes:** Minimal smoke test for cursor lifecycle on a layered URI. Would catch crashes or errors during `session.open_cursor("layered:...")` or `cursor.close()` before any data has been inserted. Runs in disagg mode with `verbose=[layered]` logging enabled.
