# test_compat03 — Compatibility API: version constraints on initial database creation

**File:** `test/suite/test_compat03.py`
**Storage mode:** General
**Components under test:** compatibility API, require_max, require_min, database creation

## Test Cases

### `test_compat03.test_compat03`
- **What it tests:** Verifies that compatibility `require_max`/`require_min` constraints work correctly at database creation time (not just reopen). When the specified release, max, or min constraints are mutually inconsistent or exceed the known version range (future_logv=20), `wiredtiger_open` raises "Version incompatibility detected".
- **Components:** `src/conn/conn_open.c`, `src/log/log_mgr.c`
- **Notes:** Cross-product of compat_release × compat_max × compat_min (14 × 8 × 8 = ~896 scenarios). Creates a new subdirectory `TEST` for each scenario. Error conditions include: log_rel >= future_logv, log_max >= future_logv, log_min >= future_logv, log_max < log_rel, log_min > log_rel, or log_max < log_min. Unlike compat02, tests creation-time (not reopen) constraint checking.
