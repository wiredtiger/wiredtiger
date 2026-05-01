# test_verbose02 — Verbose configuration API with verbosity levels (0–5)

**File:** `test/suite/test_verbose02.py`
**Storage mode:** General (some tests skipped for tiered)
**Components under test:** verbose category level parsing, verbosity level filtering (`WT_VERBOSE_INFO` through `WT_VERBOSE_DEBUG_5`)

## Test Cases

### `test_verbose02.test_verbose_single`
- **What it tests:** Opens with `verbose=[api:1]` (DEBUG_1 level); asserts `WT_VERB_API` messages are present; opens with `verbose=[api:0]` (INFO level); asserts no output (no INFO-level api messages); then tests `verbose=[compact:0]` through `verbose=[compact:5]` — all levels produce `WT_VERB_COMPACT` messages.
- **Components:** `verbose.c`, `api.c`, `compact.c`
- **Notes:** Skipped for tiered. Parameterized over flat/JSON. Level 0 = `WT_VERBOSE_INFO`, levels 1–5 = `WT_VERBOSE_DEBUG_1..5`.

### `test_verbose02.test_verbose_multiple`
- **What it tests:** Tests `verbose=[api:1,version]`, `verbose=[api,version:1]`, and `verbose=[api:1,version:1]`; for each asserts messages match `WT_VERB_API` or `WT_VERB_VERSION`.
- **Components:** `verbose.c`
- **Notes:** Parameterized over flat/JSON. Tests that per-category level settings coexist correctly.

### `test_verbose02.test_verbose_level_invalid`
- **What it tests:** Attempts `verbose=[api:-1]` (negative level) and `verbose=[api:6]` (above DEBUG_5=5); asserts `WiredTigerError` with `"Failed to parse verbose option 'api'"` for both.
- **Components:** `config.c`, `verbose.c`
- **Notes:** Parameterized over flat/JSON. Confirms bounds checking on verbosity level values.
