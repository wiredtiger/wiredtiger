# test_schema04 — Schema index with duplicate key handling

**File:** `test/suite/test_schema04.py`
**Storage mode:** General
**Components under test:** schema, index, duplicate keys, cursor

## Test Cases

### `test_schema04.test_index`
- **What it tests:** Creates a table with an index on a non-unique column (allowing duplicates). Tests three scenarios: (1) create index before populate, (2) create index during populate, (3) create index after populate. Verifies that index cursor iteration correctly returns all rows (including duplicates) and that duplicate keys are handled as expected by the index.
- **Components:** `src/schema/schema_create.c`, `src/cursor/cur_index.c`, `src/btree/`
- **Notes:** Parametrized on index creation timing (before/during/after populate). Verifies that duplicate index entries (same index key for different table keys) are correctly maintained and queryable.
