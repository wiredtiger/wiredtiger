# test_log03 — Log dirty_max configuration effect on fsync frequency

**File:** `test/suite/test_log03.py`
**Storage mode:** General (logging enabled)
**Components under test:** logging (`src/log/`), OS dirty page cache, fsync statistics

## Test Cases

### `test_log03.test_dirty_max`
- **What it tests:** Verifies that setting `log=(os_cache_dirty_pct=N)` causes more fsyncs as `N` decreases (more frequent syncing of dirty log pages). Establishes a baseline sync count with `dirty_pct=0` (no limit), then confirms progressive increase in `stat.conn.fsync_io` for percentages 50%, 33%, 25%, and 20%.
- **Components:** `src/log/log.c`, `src/os_posix/os_fs.c`, `src/stat/`
- **Notes:** Each iteration recreates a fresh home directory to ensure the log starts at offset 0, making calculations predictable. Writes 20000 rows with a 10KB value string to generate significant log traffic. The expected increases are conservative: baseline + 5, baseline + 10, baseline + 15, baseline + 20 respectively — accounting for variability in actual sync timing. Connection uses `transaction_sync=(enabled=false,method=none)` to isolate fsync counting to dirty-page syncs only. This test overrides `setUpConnectionOpen`/`setUpSessionOpen` to manage connections manually.
