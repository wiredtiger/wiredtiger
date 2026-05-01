# test_alter04 — Alter os_cache_max and os_cache_dirty_max after table creation

**File:** `test/suite/test_alter04.py`
**Storage mode:** General | Tiered
**Components under test:** schema/alter, metadata, session API, OS cache settings

## Test Cases

### `test_alter04.test_alter04_cache`
- **What it tests:** Creates a file/table with an initial `os_cache_max` or `os_cache_dirty_max` setting (or default 0), then alters the setting to `1M` and `100K` in succession, verifying the metadata reflects the new value after each alter. Tests both simple tables and complex tables (colgroup/index sub-URIs). Optionally reopens the connection after each alter to confirm persistence.
- **Components:** `src/schema/schema_alter.c`, `src/meta/meta_table.c`, `src/os_posix/os_fs.c`
- **Notes:** Parametrized across 4 URI types × 3 initial sizes (default, 1M, 200K) × 2 reopen modes × 2 settings (os_cache_max, os_cache_dirty_max) × N tiered sources. Skips file: URI under tiered storage.
