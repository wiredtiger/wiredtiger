# test_compat02 — Compatibility API: require_max/require_min version constraints on reopen

**File:** `test/suite/test_compat02.py`
**Storage mode:** General
**Components under test:** compatibility API, require_max, require_min, WAL log versions

## Test Cases

### `test_compat02.test_compat02`
- **What it tests:** Verifies that reopening a database with `require_max` and/or `require_min` compatibility constraints raises "Version incompatibility detected" when the on-disk log version falls outside the allowed range. Tests all combinations of initial `release`, `require_max`, `require_min`, and reopen `release` across known log versions 1–5 and a simulated future version (20.0).
- **Components:** `src/conn/conn_open.c`, `src/log/log_mgr.c`
- **Notes:** Cross-product of ~5400 scenarios pruned to 100 with `prune=100,prunelong=100000`. Derives expected error from log version arithmetic: error if log_min >= future_logv, log_max >= future_logv, log_max < log_rel, log_min > log_rel, log_max < log_min, log_max < log_create, or log_min > log_create. Tests `config_base=true/false` variants. Uses verbose `checkpoint` on close to debug rare hangs.
