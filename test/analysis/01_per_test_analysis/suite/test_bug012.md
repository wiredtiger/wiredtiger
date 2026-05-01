# test_bug012 — Illegal collator, key format, value format, and compressor detection

**File:** `test/suite/test_bug012.py`
**Storage mode:** General
**Components under test:** session create, configuration validation

## Test Cases

### `test_bug012.test_illegal_collator`
- **What it tests:** Attempts to create a table with `collator="xyzzy"` and verifies that `WiredTigerError` is raised with the message `unknown collator`.
- **Components:** `src/session/session_api.c`, `src/config/config_api.c`

### `test_bug012.test_illegal_key_format`
- **What it tests:** Attempts to create a table with `key_format="xyzzy"` and verifies that `WiredTigerError` is raised with the message `Invalid type`.
- **Components:** `src/session/session_api.c`, `src/pack/pack_api.c`

### `test_bug012.test_illegal_value_format`
- **What it tests:** Attempts to create a table with `value_format="xyzzy"` and verifies that `WiredTigerError` is raised with the message `Invalid type`.
- **Components:** `src/session/session_api.c`, `src/pack/pack_api.c`

### `test_bug012.test_illegal_compressor`
- **What it tests:** Attempts to create a table with `block_compressor="xyzzy"` and verifies that `WiredTigerError` is raised with the message `unknown compressor`.
- **Components:** `src/session/session_api.c`, `src/config/config_api.c`

**Notes:** Non-parametrized. All four sub-tests are independent; each creates `table:A` with a deliberately bad configuration string.
