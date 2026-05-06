# test_cursor06 — Cursor reconfigure (overwrite, readonly flags)

**File:** `test/suite/test_cursor06.py`
**Storage mode:** General
**Components under test:** cursor reconfigure API, overwrite flag, readonly flag

## Test Cases

### `test_cursor06.test_reconfigure_overwrite`
- **What it tests:** `cursor.reconfigure("overwrite=true/false")` — verifies that enabling overwrite allows duplicate inserts without error, and disabling it causes `WT_DUPLICATE_KEY` on duplicate insert.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Tagged `cursors:reconfigure`. Scenarios: file-r, file-S, table-r, table-S, table-r-complex, table-S-complex. Skipped for timestamp hook.

### `test_cursor06.test_reconfigure_readonly`
- **What it tests:** `cursor.reconfigure("readonly=true")` — verifies that a readonly cursor rejects insert/update/remove with an error.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Same scenario matrix.

### `test_cursor06.test_reconfigure_invalid`
- **What it tests:** `cursor.reconfigure()` with an invalid config string; expects `EINVAL`.
- **Components:** `src/cursor/cur_std.c`, `src/config/`
