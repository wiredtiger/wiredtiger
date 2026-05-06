# test_stat07 — Session statistics cursor configuration and reset

**File:** `test/suite/test_stat07.py`
**Storage mode:** General
**Components under test:** session statistics cursor (`statistics:session`), session reset

## Test Cases

### `test_stat_cursor_config.test_stat_cursor_config`
- **What it tests:** For each combination of database-level (`none`, `all`, `fast`) and cursor-level (`empty`, `all`, `fast`) configurations, verifies that opening `statistics:session` either succeeds or raises `'database statistics configuration'` error. When valid, also verifies that after `session.reset()` all stat values are zero.
- **Components:** `stat.c`, `session.c`
- **Notes:** Only `file:` URI used. The valid combinations mirror the connection-level stat cursors but `size` is not tested for session stats. Parameterized over 3 data_config × 3 cursor_config scenarios.
