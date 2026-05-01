# test_cache_evict_config01 — Dynamic reconfiguration of cache eviction control flags

**File:** `test/suite/test_cache_evict_config01.py`
**Storage mode:** General
**Components under test:** eviction subsystem, connection reconfiguration (`conn.reconfigure`)

## Test Cases

### `test_cache_evict_config01.test_cache_eviction_reconfig`
- **What it tests:** Verifies that all `eviction=[]` sub-configuration flags can be changed at runtime via `conn.reconfigure()` without requiring a restart, and that invalid parameter values are correctly rejected.
- **Components:** `src/conn/conn_reconfig.c`, `src/evict/evict_lru.c`
- **Notes:** Exercises eight valid configuration combinations covering `incremental_app_eviction`, `prefer_scrub_eviction`, `app_eviction_min_cache_fill_ratio` (0–50), `cache_tolerance_for_app_eviction` (0–100), and `skip_update_obsolete_check`. Also asserts `WiredTigerError` for five invalid configurations (negative values, out-of-range ratios). Performs insert and read verification after each valid reconfiguration to confirm the connection remains functional.
