# test_config10 — WiredTiger version file handling (missing/empty, with and without salvage)

**File:** `test/suite/test_config10.py`
**Storage mode:** General
**Components under test:** connection open, version file, salvage

## Test Cases

### `test_config10.test_missing_version_file`
- **What it tests:** Opening an existing WiredTiger home with the `WiredTiger` version file deleted; expects an error.
- **Components:** `src/conn/conn_open.c`

### `test_config10.test_empty_version_file`
- **What it tests:** Opening with a zero-length `WiredTiger` version file; expects an error.
- **Components:** `src/conn/conn_open.c`

### `test_config10.test_missing_version_file_with_salvage`
- **What it tests:** Opening with missing version file when `salvage=true`; verifies salvage can recover.
- **Components:** `src/conn/conn_open.c`, `src/btree/bt_salvage.c`

### `test_config10.test_empty_version_file_with_salvage`
- **What it tests:** Opening with empty version file when `salvage=true`; verifies salvage can recover.
- **Components:** `src/conn/conn_open.c`, `src/btree/bt_salvage.c`
