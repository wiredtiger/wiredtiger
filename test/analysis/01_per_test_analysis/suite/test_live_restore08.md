# test_live_restore08 — Bulk cursor usage with live restore

**File:** `test/suite/test_live_restore08.py`
**Storage mode:** General (Unix only)
**Components under test:** live restore, bulk cursor, file migration

## Test Cases

### `test_live_restore08.test_live_restore_complete_with_bulk`
- **What it tests:** Verifies that after live restore completes for a database containing a pre-existing (but empty) file, attempting to open a bulk cursor on that file fails with the expected error — because bulk load is only valid on newly created objects, not on files that have been migrated by live restore.
- **Components:** `src/live_restore/`, `src/cursor/cur_bulk.c`
- **Notes:** Setup: populates `file:standard` with 10000 rows, creates an empty `file:bulk` (no data). Backs up to SOURCE. Cleans working directory. Opens live restore connection with 1 thread and `read_size=512B`, waits for `WT_LIVE_RESTORE_COMPLETE` (2-minute timeout). Then attempts `session.open_cursor("file:bulk", None, "bulk")` — expects error: `"bulk-load is only supported on newly created objects"`.
