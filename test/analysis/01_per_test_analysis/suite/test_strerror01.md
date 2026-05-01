# test_strerror01 — Sub-level error code string descriptions

**File:** `test/suite/test_strerror01.py`
**Storage mode:** General
**Components under test:** `session.strerror()`, sub-level error codes

## Test Cases

### `test_strerror.test_strerror`
- **What it tests:** For each of the 14 WiredTiger sub-level error codes (e.g. `WT_NONE`, `WT_WRITE_CONFLICT`, `WT_CACHE_OVERFLOW`, `WT_CONFLICT_BACKUP`, `WT_CONFLICT_DISAGG`, etc.), calls `session.strerror(code)` and asserts the returned string exactly matches the expected human-readable description.
- **Components:** `error.c`, `session.c`
- **Notes:** Covers all `WT_*` sub-error codes defined in the Python bindings as of the test date. No table or data setup required.
