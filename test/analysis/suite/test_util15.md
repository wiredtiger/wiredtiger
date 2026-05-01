# test_util15 — wt alter CLI: modifying table access pattern hints

**File:** `test/suite/test_util15.py`
**Storage mode:** General
**Components under test:** `wt alter`, metadata update, `access_pattern_hint`

## Test Cases

### `test_util15.test_alter_process`
- **What it tests:** Creates a table; runs `wt alter table:<name> access_pattern_hint=sequential`; opens a `metadata:create` cursor and verifies `access_pattern_hint=sequential` appears in the stored configuration string; runs `wt alter` again with `access_pattern_hint=random`; verifies the metadata is updated to the new value.
- **Components:** `util_alter.c`, `meta.c`, `schema.c`
- **Notes:** No parameterization. Tests that `wt alter` correctly modifies and persists metadata configuration.
