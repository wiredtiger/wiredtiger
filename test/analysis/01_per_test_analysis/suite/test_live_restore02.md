# test_live_restore02 — Background thread migration until completion

**File:** `test/suite/test_live_restore02.py`
**Storage mode:** General (Unix only)
**Components under test:** live restore background migration, statistics, file creation during migration

## Test Cases

### `test_live_restore02.test_live_restore02`
- **What it tests:** Populates 3 collections (`file:foo`, `file:bar`, `file:cat`) in a source database, opens a live restore connection in a new destination directory with 1 background thread, polls `stat.conn.live_restore_state` until it reaches `WT_LIVE_RESTORE_COMPLETE` (2-minute timeout), then validates that all collections in source and destination match exactly.
- **Components:** `src/live_restore/`, `src/stat/`, `src/session/session_api.c`
- **Notes:** Parameterized by:
  - Key format: `column` (`key_format='r'`) or `row_integer` (`key_format='i'`)
  - Read size: `512B`, `4KB`, `1MB`

  Verbose mode `live_restore_progress:1` is enabled; progress messages are pre-registered as ignored patterns. During the polling loop, the test also stresses the file create path by creating a new `file:abc{i}` on each iteration. Validates absence of `.stop` files in `WT_DEST/`. Checks row-by-row key/value equality between source and destination for all 3 collections.
