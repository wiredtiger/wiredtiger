# test_bug015 — WT-2162: index drop order causes NULL pointer dereference

**File:** `test/suite/test_bug015.py`
**Storage mode:** General
**Components under test:** schema — index create/drop ordering

## Test Cases

### `test_bug015.test_bug015`
- **What it tests:** Reproduces WT-2162 where dropping two indexes on the same table in a specific order triggered a NULL pointer dereference. Creates a table with two indexes (`index:test_bug015:aab` and `index:test_bug015:aaa`), drops `aab`, recreates `aab`, drops `aaa`, recreates `aaa`. The test passes if no crash or error occurs during these operations.
- **Components:** `src/schema/schema_drop.c`, `src/schema/schema_create.c`
- **Notes:** Non-parametrized. The reproduce sequence requires the indexes to be named such that one sorts before the other alphabetically.
