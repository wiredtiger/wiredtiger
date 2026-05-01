# test_cursor_bound16 — Cursor bound with dump cursors (dump=print, dump=hex)

**File:** `test/suite/test_cursor_bound16.py`
**Storage mode:** General
**Components under test:** cursor bound API, dump cursor (dump=print, dump=hex), bound traversal

## Test Cases

### `test_cursor_bound16.test_dump_cursor`
- **What it tests:** Sets bounds on a dump cursor (opened with `dump=print` or `dump=hex` config) and exercises: forward traversal, `search_near()`, `search()`, `cursor.reset()`, and `cursor.bound("action=clear")`. Verifies that dump-encoded keys (printable or hex-escaped) are correctly handled by the bound comparison logic.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_dump.c`
- **Notes:** Scenarios: file/table × dump_print/dump_hex. Dump cursors encode keys and values as printable text or hex strings; bound keys must be supplied in the same encoding.
