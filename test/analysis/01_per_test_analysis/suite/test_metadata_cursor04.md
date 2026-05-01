# test_metadata_cursor04 — metadata:create cursor returns complete config including log settings for complex tables

**File:** `test/suite/test_metadata_cursor04.py`
**Storage mode:** General (logging enabled: `log=(enabled)`)
**Components under test:** metadata:create cursor, schema, colgroup, index, log configuration propagation

## Test Cases

### `test_metadata04.test_metadata04_complex`
- **What it tests:** Creates a complex table with a column group and an index, all with `log=(enabled=false)`. Uses the `metadata:create` cursor to fetch the full create config for each sub-object and verifies that `log=(enabled=false)` appears in the colgroup and index metadata but not (as an assertion) in the top-level table config string.
- **Components:** `src/cursor/cur_metadata.c`, `src/schema/schema_colgroup.c`, `src/schema/schema_index.c`
- **Notes:** Table: `key_format=S,value_format=SS,columns=(key,s0,s1),colgroups=(c1)`. Index: `index:metadata04:s0,columns=(s0)`. Colgroup: `colgroup:metadata04:c1,columns=(s0,s1)`. The `check_meta` helper opens `metadata:create`, searches by URI, retrieves the value, and checks for `log=(enabled=false)`.

### `test_metadata04.test_metadata04_table`
- **What it tests:** Creates a simple table with `log=(enabled=false)` and verifies that `metadata:create` returns the full config string containing `log=(enabled=false)`.
- **Components:** `src/cursor/cur_metadata.c`
- **Notes:** Single table `table:metadata04`, `key_format=S,value_format=S`. Simpler version of the complex test to cover the non-colgroup path.
