# test_import07 — Import returns error for unsupported data source types

**File:** `test/suite/test_import07.py`
**Storage mode:** General
**Components under test:** schema/import, data source type validation

## Test Cases

### `test_import07.test_import_unsupported_data_source`
- **What it tests:** Verifies that attempting to import using a URI prefix other than `file:` or `table:` returns `ENOTSUP` ("Operation not supported").
- **Components:** `src/schema/schema_create.c`
- **Notes:** Parameterized by:
  - `colgroup` — URI prefix `colgroup:`
  - `index` — URI prefix `index:`

  The target URI does not actually exist; the test only checks that the import path performs the data-source type check before any other validation. Expected error: `"Operation not supported"`.
