# test_turtle01 — WiredTiger.turtle file validation

**File:** `test/suite/test_turtle01.py`
**Storage mode:** General
**Components under test:** `WiredTiger.turtle` file, metadata, version strings, checkpoint metadata

## Test Cases

### `test_turtle01.test_validate_turtle_file`
- **What it tests:** Reads and validates the `WiredTiger.turtle` file for an empty database; then creates a table, inserts 1,000 rows, checkpoints; reads and validates the turtle file again. Validation checks: (1) the `WiredTiger version string` key matches format `major.minor.patch`; (2) the `WiredTiger version` key matches format `major=N,minor=N,patch=N`; (3) the version numbers from both keys are identical; (4) the last line of the turtle file (checkpoint metadata) contains comma-separated fields.
- **Components:** `meta.c`, `checkpoint.c`, `os_fs.c`
- **Notes:** Directly reads `WiredTiger.turtle` from the filesystem via Python `open()`. Tests the structure and correctness of the turtle file metadata both before and after data is inserted and checkpointed.
