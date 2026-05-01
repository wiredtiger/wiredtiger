# test_tiered20 — Conflict detection when two connections write to the same bucket object

**File:** `test/suite/test_tiered20.py`
**Storage mode:** Tiered and non-tiered (all scenarios)
**Components under test:** bucket overwrite protection (no-overwrite policy), `debug_mode=(tiered_flush_error_continue=true)`, multi-connection shared bucket via symlink (dir_store), data integrity after flush conflict

## Test Cases

### `test_tiered20.test_tiered_overwrite`
- **What it tests:** Verifies that the tiered storage layer prevents a second connection from overwriting an object already written to the bucket by a first connection, and that data from the first writer is preserved. Test structure:
  1. **Full-drop-recreate cycle:** Creates, flushes, and drops (with `remove_shared=true`) a table 3 times to confirm clean round-trips work.
  2. **Drop without shared removal:** Creates `uri_c`, flushes, drops with `remove_shared=false` (local metadata gone but bucket object remains). Re-creates the same URI — expects `EEXIST` error because the bucket object exists.
  3. **Two-system conflict:** Opens a second WiredTiger home directory (`SECOND`). For dir_store, symlinks `SECOND/bucket1` to the shared `bucket1`. Creates the same URI (`uri_b`) in both connections. Inserts different data (first: `"APPLES_IN_THIS_FILE!" * 1000`; second: `"SOMETHING_VERY_DIFFERENT!" * 1000`). First connection flushes successfully. Second connection's flush is expected to fail with `EEXIST` (detected via `expectedStderrPattern`). For dir_store, verifies the bucket object still contains "APPLES" and not "DIFFERENT". After local objects are removed (local_retention expiry), the first connection can still read its data correctly from the bucket.
- **Components:** `src/tiered/conn_tiered.c` (flush conflict detection), `ext/storage_sources/dir_store` (EEXIST on overwrite), `debug_mode=(tiered_flush_error_continue)` (prevents assertion crash on flush error), `local_retention=1`, `tiered_interval=5`
- **Notes:**
  - Parametrized across all storage sources (including non_tiered — non-tiered scenario only runs the drop-recreate cycle then skips the rest).
  - Currently skipped for non-local (cloud) backends due to FIXME-WT-11004 (cloud drivers not yet implementing no-overwrite).
  - `debug_mode=(tiered_flush_error_continue=true)` is essential: without it, a flush failure causes an assertion in the background thread which crashes the test process.
  - `file_contains` helper reads binary file content to verify data identity in dir_store objects.
