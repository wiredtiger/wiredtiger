# test_cache_evict_config02 — Behavioral verification of prefer_scrub_eviction flag

**File:** `test/suite/test_cache_evict_config02.py`
**Storage mode:** General
**Components under test:** eviction subsystem, cache statistics (`stat.conn.cache_write_restore_scrub`)

## Test Cases

### `test_cache_evict_config02.test_cache_eviction_reconfig_and_scrub`
- **What it tests:** Verifies that enabling `prefer_scrub_eviction=true` via `conn.reconfigure()` results in measurably more scrub-eviction activity compared to a baseline with the flag disabled.
- **Components:** `src/evict/evict_lru.c`, `src/evict/evict_page.c`
- **Notes:** Uses a 5 MB cache with 50 000 repeated updates over 100 keys to force dirty-page pressure. Captures `cache_write_restore_scrub` statistics before and after enabling the flag; asserts the post-flag count is strictly greater than the baseline. This is a statistical correctness test confirming the scrub-preference heuristic is exercised rather than just configured.
