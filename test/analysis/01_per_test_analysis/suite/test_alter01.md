# test_alter01 — Smoke-test session.alter for access_pattern_hint and cache_resident

**File:** `test/suite/test_alter01.py`
**Storage mode:** General | Tiered
**Components under test:** schema/alter, metadata, session API

## Test Cases

### `test_alter01.test_alter01_access`
- **What it tests:** Creates a file/table with initial `access_pattern_hint` and `cache_resident` settings, then cycles through all combinations of those settings via `session.alter()`, verifying that each change is reflected correctly in the metadata cursor. Also covers optional connection reopen after each alter.
- **Components:** `src/schema/schema_alter.c`, `src/meta/meta_table.c`
- **Notes:** Parametrized across 4 URI types (file, table+colgroup, table+index, table-simple) × 4 access hints (default, none, random, sequential) × 3 cache_resident values (default, false, true) × 2 reopen settings × N tiered storage sources. Skips file: URI under tiered storage. Verifies both simple and complex (colgroup/index) sub-URIs.
