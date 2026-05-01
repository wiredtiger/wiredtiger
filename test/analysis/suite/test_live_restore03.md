# test_live_restore03 — live_restore fs_size returns valid size for source-only files

**File:** `test/suite/test_live_restore03.py`
**Storage mode:** General (Unix only)
**Components under test:** live restore file system, statistics (`block_size`)

## Test Cases

### `test_live_restore03.test_live_restore03`
- **What it tests:** Opens a live restore connection with `threads_max=0` (no background migration) so files remain only in the source directory. Opens a statistics cursor with `statistics=(size)` on each URI and verifies that `stat.dsrc.block_size` is greater than 0 — meaning `live_restore->fs_size` can return a valid size even when the file has not yet been migrated to the destination.
- **Components:** `src/live_restore/live_restore_fs.c`, `src/stat/`
- **Notes:** Tests both a `file:` URI and a `table:` URI to cover both data source types. 100 rows, `key_format='i', value_format='S'`. No background threads intentionally avoids opening file handles during the test.
