# test_util19 — wt downgrade CLI: compatibility version downgrade and log format version

**File:** `test/suite/test_util19.py`
**Storage mode:** General
**Components under test:** `wt downgrade`, compatibility version management, log format version

## Test Cases

### `test_util19.test_downgrade`
- **What it tests:** Creates a database at a specified compatibility release (none/10.0/3.3/3.2/3.1/3.0/2.6); populates 100 rows; runs `wt downgrade -V <downgrade_rel>` with `verbose=[log]`; checks the verbose log output for `COMPATIBILITY: Version now <N>` where N is the expected log compatibility version; if the downgraded version equals the latest (5), confirms the message does NOT appear (no-op downgrade).
- **Components:** `util_downgrade.c`, `conn.c`, `log.c`
- **Notes:** Parameterized over 7 create releases × 6 downgrade releases = 42 scenarios. Log format compatibility versions: 2.6→1, 3.0→2, 3.1/3.2→3, 3.3→4, 10.0→5 (latest). Tests the log version reconfiguration path via `WT_CONNECTION.reconfigure`.
