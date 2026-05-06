# test_metadata_cursor02 — Metadata cursor behavior with partially dropped (invalid) tables

**File:** `test/suite/test_metadata_cursor02.py`
**Storage mode:** General (skipped for disagg and tiered hooks)
**Components under test:** metadata cursor, schema repair, incomplete table detection

## Test Cases

### `test_metadata_cursor02.test_missing`
- **What it tests:** Creates 3 tables, invalidates one by dropping either its colgroup or underlying file (simulating crash/partial drop), then iterates the metadata cursor and verifies the correct number of valid table entries is returned. For `metadata:create`, expects an error message about missing metadata.
- **Components:** `src/cursor/cur_metadata.c`, `src/meta/meta_table.c`, `src/schema/schema_drop.c`
- **Notes:** Parameterized by two axes (4 scenarios total):
  - `metauri`: `metadata:` or `metadata:create`
  - `drop`: `colgroup` (drops `colgroup:t1` etc.) or `file` (drops `file:t1.wt` etc.)

  Expected table count:
  - `metadata:` — 3 (invalid table still has its `table:` entry)
  - `metadata:create` — 2 (invalid table is filtered out; emits `"metadata information.*not found"` stderr warning)

  Each sub-test re-creates all tables fresh via `reopen_conn()` + `drop(..., force=true)` + `create(...)`. Skipped for disagg and tiered storage hooks because they manage file storage differently.
