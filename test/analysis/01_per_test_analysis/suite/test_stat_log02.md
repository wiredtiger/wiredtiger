# test_stat_log02 — Statistics log JSON format and sources argument

**File:** `test/suite/test_stat_log02.py`
**Storage mode:** General
**Components under test:** statistics log JSON output, `sources` filter

## Test Cases

### `test_stat_log02.test_stats_log_json`
- **What it tests:** Opens with `statistics_log=(wait=1,json,on_close=1)`, closes the connection, then verifies the `WiredTigerStat.*` file exists and each line is valid JSON.
- **Components:** `stat_log.c`
- **Notes:** Manual connection setup.

### `test_stat_log02.test_stats_log_on_json_with_tables`
- **What it tests:** Opens with `sources=[file:]` in the stats log config; creates a table `foo` and inserts one record; closes the connection; verifies the stats file is JSON and that it contains `file:foo.wt` under `wiredTigerTables`.
- **Components:** `stat_log.c`, `schema.c`
- **Notes:** Skipped for tiered hook. Tests the `sources` filter includes per-file table stats in JSON output.
