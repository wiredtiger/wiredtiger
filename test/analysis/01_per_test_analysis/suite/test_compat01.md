# test_compat01 — Compatibility API: release upgrade/downgrade via restart or reconfigure

**File:** `test/suite/test_compat01.py`
**Storage mode:** General
**Components under test:** compatibility API, WAL log versions, connection reconfiguration

## Test Cases

### `test_compat01.test_reconfig`
- **What it tests:** Verifies that the compatibility `release` setting can be changed at runtime via `conn.reconfigure()` (upgrade or downgrade). Checks that log files are or are not removed depending on whether the new release is a downgrade, and that `prev_lsn` records exist in the log if and only if log version >= 2.
- **Components:** `src/conn/conn_reconfig.c`, `src/log/log_mgr.c`
- **Notes:** 225-scenario cross-product of start_compat × restart_compat (15 × 15 release versions from "1.8" to "12.0"). Log versions 1–5 mapped to release ranges. For downgrade (logv2 != logv2 and not latest), original log files are removed. Uses `wt printlog` output to verify `prev_lsn` record presence. Skips scenarios where compat2='none'.

### `test_compat01.test_restart`
- **What it tests:** Same scenarios but changes compatibility by closing and reopening the connection (rather than `reconfigure`). Verifies log file state and `prev_lsn` presence after reopening with the new compatibility setting.
- **Components:** `src/conn/conn_open.c`, `src/log/log_mgr.c`
- **Notes:** Logs are kept with `remove=false` to allow checking. Reopening at a downgraded release removes incompatible newer logs.

### `test_reconfig_fail.test_reconfig_fail`
- **What it tests:** Verifies that compatibility reconfiguration is blocked when an active transaction is in progress, raising `WiredTigerError` with "system must be quiescent". Also verifies that non-compatibility reconfiguration (e.g., `cache_size`) succeeds even with an active transaction while downgraded.
- **Components:** `src/conn/conn_reconfig.c`
- **Notes:** Downgrades to release="2.6", starts a transaction, then attempts to upgrade to "3.0.0" — must fail. Confirms unrelated reconfig (`cache_size=100M`) still works.
