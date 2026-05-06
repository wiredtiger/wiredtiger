# test_empty — Empty object file-size check

**File:** `test/suite/test_empty.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** btree, block manager, schema

## Test Cases

### `test_empty.test_empty_create`
- **What it tests:** Creates a file or table object with a given key format, closes the session, and verifies the resulting `.wt` file is exactly one sector (4 KB). An empty object should write only the file header block and nothing else.
- **Components:** `src/block/`, `src/btree/`, `src/schema/`
- **Notes:** Scenarios: `file:` vs `table:`, key format `r` (recno) vs `S` (string). For `table:` URIs the check looks for `<name>.wt`. Skipped entirely under the tiered storage hook because the test inspects raw `.wt` file names and uses column store.
