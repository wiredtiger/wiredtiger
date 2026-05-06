# test_compat05 — Compatibility API: log archive vs remove flag behavior

**File:** `test/suite/test_compat05.py`
**Storage mode:** General (skips tiered)
**Components under test:** compatibility API, WAL log removal, archive flag, remove flag

## Test Cases

### `test_compat05.test_compat05`
- **What it tests:** Verifies that the `log=(archive=...)` and `log=(remove=...)` configuration flags correctly control whether old WAL log files are removed after a checkpoint. The `archive` flag is the old name for `remove`; both should work, and `remove` overrides `archive` when both are specified.
- **Components:** `src/log/log_mgr.c`, `src/conn/conn_open.c`
- **Notes:** Skip: `@wttest.skip_for_hook("tiered", ...)`. Seven scenarios testing all combinations: `archive=false` (no remove), `archive=true` (remove), default (remove), `remove=false` (no remove), `remove=true,archive=false` (remove wins), `remove=false,archive=true` (implicit: remove wins), `remove=true` (remove). Populates 10 000 rows to generate at least 2 log files. After checkpoint, polls up to 90 seconds for log1 removal. Asserts removed/not-removed matches the scenario expectation.
