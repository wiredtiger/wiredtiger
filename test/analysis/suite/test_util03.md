# test_util03 — wt create CLI: table creation and file import

**File:** `test/suite/test_util03.py`
**Storage mode:** General
**Components under test:** `wt create`, file import (`import=(enabled)`)

## Test Cases

### `test_util03.test_create_process`
- **What it tests:** Runs `wt create` with optional `-c key_format=X,value_format=Y` arguments; verifies the table is created empty and the cursor reports the correct key and value formats.
- **Components:** `util_create.c`, `schema.c`
- **Notes:** Parameterized over none/SS/rS/ri key-value format combinations. When format is `none`, creates with default formats and does not assert specific key/value formats.

### `test_util03_import.test_create_process_import`
- **What it tests:** Creates a `file:` table with `allocation_size=512,key_format=i,value_format=i` and populates 999 rows; exports the metadata via `metadata:` cursor; drops the file (keeping the `.wt` file on disk); then uses `wt create -c import=(enabled,...) file:...` to import it back; verifies all 999 rows are accessible.
- **Components:** `util_create.c`, `schema.c`, `block.c`
- **Notes:** Parameterized over `file_metadata` (repair=false, explicit file_metadata from exported config) and `repair` (repair=true, no metadata). Tests both metadata-provided and repair-based import paths.
