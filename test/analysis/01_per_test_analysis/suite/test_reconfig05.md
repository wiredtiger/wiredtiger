# test_reconfig05 — Connection reconfiguration with nested struct configs and os_cache_dirty_pct

**File:** `test/suite/test_reconfig05.py`
**Storage mode:** General (logging enabled)
**Components under test:** connection reconfiguration, configuration parsing, log OS cache dirty percentage

## Test Cases

### `test_reconfig05.test_reconfig05`
- **What it tests:** Verifies that `conn.reconfigure()` correctly parses and applies nested struct configurations that do not use the `=` separator between the key and its value; specifically tests: `cache_size=1GB` (simple), `cache_size=1GB,log=(os_cache_dirty_pct=30)` (mixed simple + nested), and `log=(os_cache_dirty_pct=50)` (nested only)
- **Components:** `config/config.c`, `conn/conn_cache.c`, `log/log.c`
- **Notes:** `conn_config = 'log=(enabled)'`; tagged with `[TEST_TAGS] session_api:reconfigure`; the test name refers to "structs without the `=` separator" in the comment, meaning config strings where a struct's value is specified inline (e.g., `log=(os_cache_dirty_pct=30)`) rather than using explicit assignment; guards against a config parser regression for nested struct reconfiguration
