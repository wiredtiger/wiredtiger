# test_base02 — Configuration string parsing: various format combinations and JSON

**File:** `test/suite/test_base02.py`
**Storage mode:** General
**Components under test:** session API (create/drop), configuration parser

## Test Cases

### `test_base02.test_config_combinations`
- **What it tests:** Spot-checks multiple combinations of allocation_size, page sizes, leaf settings, and column definitions (with various whitespace and formatting quirks) in the create configuration string. For each valid combination, creates and immediately drops the table/file.
- **Components:** `src/config/config_api.c`, `src/schema/schema_create.c`

### `test_base02.test_config_json`
- **What it tests:** Verifies that configuration strings in JSON format (generated via `json.dumps`) are accepted by `session.create()`. Tests a simple column spec and a more complex schema with key_format, value_format, columns, and colgroups.
- **Components:** `src/config/config_api.c`, `src/schema/schema_create.c`
- **Notes:** Parametrized across `file:` and `table:` URI prefixes.
