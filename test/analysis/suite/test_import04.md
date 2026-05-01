# test_import04 — Table import success and failure scenarios

**File:** `test/suite/test_import04.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** schema/import, schema/drop, error handling, metadata, timestamps

## Test Cases

### `test_import04.test_table_import`
- **What it tests:** Six sub-scenarios in one test method covering all expected success/failure paths for `table:` import:
  1. Import when URI already exists → FAILURE (WiredTigerError)
  2. Drop with `remove_files=false` then re-import into same DB → SUCCESS
  3. Import before copying the data file to destination → FAILURE (`"No such file or directory"`)
  4. Import with no table config (only `file_metadata`) → FAILURE (`"Invalid argument"`)
  5. Import with no `file_metadata` → FAILURE (`"Invalid argument"`)
  6. Full correct import with both table config and `file_metadata` → SUCCESS
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/meta/`
- **Notes:** Parameterized by:
  - `simple_table` — `key_format=r,value_format=i`
  - `table_with_named_columns` — `key_format=r,value_format=SSi`, 6-row country dataset

  Uses `remove_files=false` on drop to retain the `.wt` file for reimport. Decorated with `@wttest.skip_for_hook("tiered")`.
