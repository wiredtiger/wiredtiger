# test_import01 — Import a file into a running database (cross-DB and same-DB)

**File:** `test/suite/test_import01.py`
**Storage mode:** General
**Components under test:** schema/import, block manager, metadata, checkpoint, timestamps

## Test Cases

### `test_import01.test_file_import`
- **What it tests:** Imports a file from a separate source database into a different destination database; verifies data survives import, metadata config matches, and new writes succeed after import.
- **Components:** `src/schema/schema_create.c`, `src/btree/bt_io.c`, `src/meta/meta_ckpt.c`, `src/txn/txn_timestamp.c`
- **Notes:** Uses binary key/value format (`key_format=u,value_format=u`). Two checkpoint cycles before export. Requires `oldest_timestamp` to be advanced past imported data timestamps. Also creates a named checkpoint after import to confirm it does not corrupt the comparison. Uses `import=(enabled,repair=false,file_metadata=(...))`.

### `test_import01.test_file_import_dropped_file`
- **What it tests:** Drops a table within the same database (keeping the data file on disk), then re-imports it into the same database using the previously saved metadata.
- **Components:** `src/schema/schema_drop.c`, `src/schema/schema_create.c`, `src/meta/`
- **Notes:** Validates the "drop + reimport" workflow that is common in MongoDB server usage. File is backed up to a temporary directory before drop, then copied back. Verifies all originally inserted values are readable after reimport.

### `test_import_base` (shared base class)
- **What it tests:** Not a test itself; provides helpers (`update`, `delete`, `check_record`, `check`, `config_compare`, `strip_subconfig`, `populate`, `copy_file`) shared across all import test files.
- **Components:** N/A
- **Notes:** `config_compare` strips `id=` and `checkpoint` subconfig fields before comparing, since those are expected to differ between source and destination.
