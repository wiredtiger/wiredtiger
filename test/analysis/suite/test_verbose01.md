# test_verbose01 — Verbose configuration API: single/multiple/no/invalid categories (legacy style)

**File:** `test/suite/test_verbose01.py`
**Storage mode:** General (some tests skipped for tiered)
**Components under test:** verbose configuration API, JSON event handler output, verbose category filtering

## Test Cases

### `test_verbose01.test_verbose_single`
- **What it tests:** Opens a connection with `verbose=[api]`; performs table create and cursor insert; asserts all stdout messages match `WT_VERB_API`; then opens with `verbose=[compact]`, invokes `session.compact`, and asserts all messages match `WT_VERB_COMPACT`.
- **Components:** `verbose.c`, `api.c`, `compact.c`
- **Notes:** Skipped for tiered. Parameterized over flat and JSON output formats. Tests that enabling a single category produces only messages from that category.

### `test_verbose01.test_verbose_multiple`
- **What it tests:** Opens with `verbose=[api,version]`; performs API operations; asserts all messages match either `WT_VERB_API` or `WT_VERB_VERSION`.
- **Components:** `verbose.c`, `api.c`
- **Notes:** Parameterized over flat/JSON. Tests that multiple categories are correctly filtered.

### `test_verbose01.test_verbose_none`
- **What it tests:** Opens with no verbose categories (`verbose=[]`); performs API operations; asserts zero verbose messages are produced.
- **Components:** `verbose.c`
- **Notes:** Parameterized over flat/JSON. Tests the empty category list case.

### `test_verbose01.test_verbose_invalid`
- **What it tests:** Attempts to open a connection with `verbose=[test_verbose_invalid]`; asserts a `WiredTigerError` is raised containing `"'test_verbose_invalid' not a permitted choice for key 'verbose'"`.
- **Components:** `config.c`, `verbose.c`
- **Notes:** Parameterized over flat/JSON. Tests invalid category name rejection.
