# test_truncate26 — Error when truncate uses cursors owned by another session

**File:** `test/suite/test_truncate26.py`
**Storage mode:** General
**Components under test:** truncate cursor ownership validation, `EINVAL`

## Test Cases

### `test_cursor24.test_cursor24_truncate`
- **What it tests:** Opens start and stop cursors in both session1 and session2; then calls `session2.truncate()` with all four combinations of cursor ownership (s2+s2, s1+s2, s2+s1, s1+s1); verifies that only s2+s2 succeeds (returns 0) and all combinations involving a session1 cursor raise `WiredTigerError` with message "bounding cursors must be owned by the truncating session: Invalid argument".
- **Components:** `session.c`, `cursor.c`, `schema.c`
- **Notes:** Note: the file is named test_truncate26.py but the class name is `test_cursor24`. Tests argument validation ensuring truncate boundary cursors must belong to the same session performing the truncate.
