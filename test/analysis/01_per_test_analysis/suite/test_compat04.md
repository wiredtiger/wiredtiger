# test_compat04 — Compatibility API: reconfigure release then reopen with matching require_max

**File:** `test/suite/test_compat04.py`
**Storage mode:** General
**Components under test:** compatibility API, release reconfiguration, require_max, WAL log versions

## Test Cases

### `test_compat04.test_compat04`
- **What it tests:** Verifies that after reconfiguring a database to a new `release` version via `conn.reconfigure()`, subsequently reopening with `release=X,require_max=X` (same version) always succeeds regardless of the initial creation release or reconfig direction (upgrade or downgrade).
- **Components:** `src/conn/conn_reconfig.c`, `src/conn/conn_open.c`, `src/log/log_mgr.c`
- **Notes:** Cross-product of create_release (12 versions) × reconfig_release (11 versions) × base_config (true/false) = 264 scenarios. Creates database at `create_rel`, writes 2 000 entries to generate logs, reconfigures to `rel`, closes, then reopens with `release=rel,require_max=rel`. Must always succeed since the require_max matches the actual release. Tests the upgrade/downgrade-then-reopen round-trip pattern. Verifies `config_base` doesn't affect the result.
