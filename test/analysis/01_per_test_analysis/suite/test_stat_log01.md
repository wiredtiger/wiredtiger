# test_stat_log01 — Statistics log file creation and configuration

**File:** `test/suite/test_stat_log01.py`
**Storage mode:** General
**Components under test:** statistics log (`statistics_log`), connection configuration

## Test Cases

### `test_stat_log01.test_stats_log_default`
- **What it tests:** Opens a connection with `statistics=(fast),statistics_log=(wait=1)`, waits 2 seconds, and verifies that a `WiredTigerStat.*` file is created in the default directory.
- **Components:** `stat_log.c`, `conn.c`
- **Notes:** Manual connection setup.

### `test_stat_log01.test_stats_log_name`
- **What it tests:** Opens with `statistics_log=(wait=1,path=foo)`, waits 2 seconds, and verifies a stats file is created in the `foo` subdirectory.
- **Components:** `stat_log.c`
- **Notes:** Tests path override.

### `test_stat_log01.test_stats_log_on_close_and_log`
- **What it tests:** Opens with `on_close=true,wait=1`; after 2 seconds closes the connection; verifies the stats file exists (written both periodically and on-close).
- **Components:** `stat_log.c`
- **Notes:** Ensures on-close and periodic logging can coexist.

### `test_stat_log01.test_stats_log_on_close`
- **What it tests:** Opens with `on_close=true` only (no `wait`); closes the connection; verifies the stats file exists.
- **Components:** `stat_log.c`
- **Notes:** Tests that on-close writing works without the background thread.

### `test_stat_log01_readonly.test_stat_log01_readonly`
- **What it tests:** Creates a database with logging and `statistics_log=(on_close=true)`, closes it, then reopens in readonly mode to verify the configuration persists without crashing.
- **Components:** `stat_log.c`, `conn.c`
- **Notes:** Skipped for tiered hook. Validates that readonly reopens with stats log config do not error.
