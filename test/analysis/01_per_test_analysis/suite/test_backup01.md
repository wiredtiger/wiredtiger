# test_backup01 — Basic backup cursor, wt backup command, and checkpoint interaction

**File:** `test/suite/test_backup01.py`
**Storage mode:** General
**Components under test:** backup cursor, backup utility (wt), checkpoint, schema (file/table)

## Test Cases

### `test_backup.test_cursor_simple`
- **What it tests:** Opens and closes a `backup:` cursor without error.
- **Components:** `src/cursor/cur_backup.c`

### `test_backup.test_cursor_single`
- **What it tests:** Verifies that attempting to open a second `backup:` cursor while one is already open raises `/there is already a backup cursor open/`.
- **Components:** `src/cursor/cur_backup.c`

### `test_backup.test_backup_database`
- **What it tests:** Populates a mix of file and table objects (SimpleDataSet + ComplexDataSet), runs the `wt backup` utility command to a new directory, then uses `wt list` on both directories to verify all objects were copied and their contents are identical.
- **Components:** `src/cursor/cur_backup.c`, `src/utils/util_backup.c`

### `test_backup.test_backup_table`
- **What it tests:** Tests selective backup using `wt backup -t <uri>`. Runs multiple subsets of the 6 test objects, confirming that included objects are present with correct content and excluded objects are absent in the backup directory.
- **Components:** `src/utils/util_backup.c`

### `test_backup.test_cursor_reset`
- **What it tests:** Opens a backup cursor, iterates all files once, calls `cursor.reset()`, iterates again, and verifies the total count equals exactly twice the first-pass count.
- **Components:** `src/cursor/cur_backup.c`

### `test_backup.test_checkpoint_delete`
- **What it tests:** Verifies that with an open backup cursor: (1) unnamed checkpoints can be created; (2) named checkpoints created before the backup cursor was opened cannot be dropped (EBUSY); (3) named checkpoints created after the backup cursor is opened can be dropped; (4) after closing the backup cursor, previously pinned named checkpoints can be deleted. Uses a 2-second sleep to avoid timestamp collision with the backup cursor time.
- **Components:** `src/cursor/cur_backup.c`, `src/session/session_api.c`, `src/checkpoint/checkpoint.c`
