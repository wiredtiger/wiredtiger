# test_live_restore06 — Live restore cleans all nbits=-1 strings from file metadata during backups

**File:** `test/suite/test_live_restore06.py`
**Storage mode:** General (Unix only)
**Components under test:** live restore cleanup phase, metadata, backup, sweep server

## Test Cases

### `test_live_restore06.test_live_restore06`
- **What it tests:** Verifies that once live restore completes, all file metadata entries contain `nbits=-1` (the live restore sentinel), and that subsequent backups strip those sentinel values — replacing them with `nbits=0` in the backup copy's metadata.
- **Components:** `src/live_restore/live_restore_cleanup.c`, `src/meta/`, `src/backup/backup_cursor.c`, `src/conn/conn_sweep.c`
- **Notes:** Uses `timing_stress_for_test=[live_restore_clean_up]` which adds a 4-second sleep in the cleanup phase, giving the sweep server (configured with `close_idle_time=1,close_scan_interval=1,close_handle_minimum=1`) time to close file handles before the final forced checkpoint at end of cleanup.

  Two backup scenarios tested via `do_backup_test`:
  1. `backup0` — backup while live restore is in COMPLETE phase (connection opened with `enabled=true`).
  2. `backup1` — backup in non-live restore mode (`enabled=false`).

  For each backup:
  - Opens backup cursor, takes full backup.
  - Reads `WiredTiger.backup` text file and asserts `"nbits=-1"` does not appear.
  - Reopens the backup directory and reads metadata cursor; for every `file:` URI asserts `"nbits=0,"` is present.
