# test_config06 — Session create key/value format config and storage tier config validation

**File:** `test/suite/test_config06.py`
**Storage mode:** General
**Components under test:** session create API, schema (key/value format strings), config validation

## Test Cases

### `test_config06.test_bad_session_config`
- **What it tests:** Invalid session config string; expects error.
- **Components:** `src/config/`, `src/session/`

### `test_config06.test_format_string_S_1` / `test_format_string_S_4` / `test_format_string_S_10`
- **What it tests:** Fixed-length string format `S` with sizes 1, 4, 10 in session.create.
- **Components:** `src/schema/`, `src/config/`

### `test_config06.test_format_string_s_1` / `test_format_string_s_4` / `test_format_string_s_10`
- **What it tests:** Variable-length string format `s` with sizes 1, 4, 10 in session.create.
- **Components:** `src/schema/`, `src/config/`

### `test_config06.test_format_string_S_default` / `test_format_string_s_default`
- **What it tests:** Default (unspecified size) for fixed and variable string formats.
- **Components:** `src/schema/`, `src/config/`

### `test_config06.test_storage_tier_config_in_asc`
- **What it tests:** Storage tier config options when used in ASC (non-disagg) mode; verifies acceptance or rejection.
- **Components:** `src/config/`, `src/conn/`
- **Notes:** Skipped for disagg hook.
