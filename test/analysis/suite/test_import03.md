# test_import03 — Import a table (not just a file) into a running database

**File:** `test/suite/test_import03.py`
**Storage mode:** General
**Components under test:** schema/import, table layer, metadata, checkpoint, timestamps

## Test Cases

### `test_import03.test_table_import`
- **What it tests:** Imports a full `table:` object (not just a raw `file:`) from one database directory to another, verifying that both table-level and file-level metadata are preserved and data is correct after import.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_table.c`, `src/meta/meta_ckpt.c`
- **Notes:** Parameterized by two scenarios:
  - `simple_table` — `key_format=r,value_format=i`, 100 rows of random integers.
  - `table_with_named_columns` — `key_format=r,value_format=SSi`, 6 rows with country/capital/population tuples, uses `columns=(id,country,capital,population)`.

  Uses `import=(enabled,repair=false,file_metadata=(...))` combined with the original table config. Two checkpoint cycles before export. Validates that post-import writes succeed and a final checkpoint completes.
