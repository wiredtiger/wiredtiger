# test_hs04 — History store file_max configuration and reconfiguration

**File:** `test/suite/test_hs04.py`
**Storage mode:** General
**Components under test:** history store (file_max config), connection reconfigure, statistics

## Test Cases

### `test_hs04.test_hs`
- **What it tests:** Opens the connection with various initial `history_store=(file_max=...)` settings and `in_memory` modes, checks the `cache_hs_ondisk_max` statistic matches the configured value, then reconfigures to a new `file_max` and verifies the stat updates. Specifically:
  - For `in_memory=true`, the HS ondisk max stat is always 0 regardless of config.
  - For non-in-memory, the stat reflects the configured value in bytes (0 for default/zero, WT_MB*100 for "100MB").
  - Reconfiguring to `file_max=99MB` (below the minimum of 100MB) raises an error matching `/below minimum/`.
- **Components:** `src/history/`, `src/conn/`
- **Notes:** Scenarios: 3 `in_memory` values × 3 `init_file_max` values × 3 `reconfig_file_max` values = 27. `WT_MB = 1048576`. The test returns early after the error check if `reconfig_stat_val is None`.
