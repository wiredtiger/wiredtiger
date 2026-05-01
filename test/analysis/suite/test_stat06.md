# test_stat06 — Statistics enabled/disabled at open time

**File:** `test/suite/test_stat06.py`
**Storage mode:** General
**Components under test:** connection statistics configuration, statistics cursor

## Test Cases

### `test_stat06.test_stats_on`
- **What it tests:** Opens a connection with `statistics=(fast)`, then confirms that a statistics cursor can be opened and `file_open` is greater than zero.
- **Components:** `conn.c`, `stat.c`
- **Notes:** Manually opens connection with `wiredtiger_open`.

### `test_stat06.test_stats_off`
- **What it tests:** Opens a connection with `statistics=(none)` but `statistics_log=(json)`, then confirms that attempting to open a statistics cursor raises `'database statistics configuration'` error.
- **Components:** `conn.c`, `stat.c`
- **Notes:** Verifies that the statistics log can be configured without enabling statistics collection, and that cursor access is blocked.
