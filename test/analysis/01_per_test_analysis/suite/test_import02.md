# test_import02 — Error conditions when importing files

**File:** `test/suite/test_import02.py`
**Storage mode:** General
**Components under test:** schema/import, error handling, metadata validation

## Test Cases

### `test_import02.test_file_import_empty_metadata`
- **What it tests:** Attempts to import a file with `file_metadata=""` (empty string); expects WT to reject with an error requiring `file_metadata` or `metadata_file` to be specified.
- **Components:** `src/schema/schema_create.c`
- **Notes:** Uses `no_metadata_helper` which sets up a cross-database import scenario. Expected error: `"import requires that 'file_metadata' or 'metadata_file' is specified"`.

### `test_import02.test_file_import_no_metadata`
- **What it tests:** Attempts to import a file with `import=(enabled,repair=false)` but no `file_metadata` or `metadata_file` option at all; expects same error as empty metadata case.
- **Components:** `src/schema/schema_create.c`
- **Notes:** Same helper as above. Confirms that the absence of the metadata config key and an empty string are treated equivalently.

### `test_import02.test_file_import_existing_uri`
- **What it tests:** Attempts to import a file whose URI already exists in the destination database; expects an error because the table is already present.
- **Components:** `src/schema/schema_create.c`, `src/meta/`
- **Notes:** No cross-database move; imports into the same connection where the table was created. The import should fail without dropping first.

### `test_import02.test_import_file_missing_file`
- **What it tests:** Attempts to import with valid `file_metadata` but without copying the actual `.wt` data file to the destination directory.
- **Components:** `src/schema/schema_create.c`, `src/os_posix/os_open.c`
- **Notes:** Expects `"No such file or directory"` error. Confirms that metadata-only import without the underlying data file fails gracefully.
