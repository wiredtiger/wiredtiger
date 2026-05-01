# test_excl — session.create() with exclusive=true flag

**File:** `test/suite/test_excl.py`
**Storage mode:** General (tiered storage supported; file: URI skipped for tiered)
**Components under test:** schema (create)

## Test Cases

### `test_create_excl.test_create_excl`
- **What it tests:** Verifies the `exclusive` configuration option for `session.create()`:
  1. Creating a URI with `exclusive=true` succeeds when the object does not exist.
  2. Creating the same URI again with `exclusive=true` raises `WiredTigerError` (object already exists).
  3. Creating the same URI with `exclusive=false` succeeds (non-exclusive re-create is allowed).
  4. Creating a new, non-existent URI with `exclusive=true` succeeds.
  5. Creating another new URI with `exclusive=false` succeeds.
- **Components:** `src/schema/`
- **Notes:** Scenarios: URI type `file:` vs `table:`; tiered storage sources from `gen_tiered_storage_sources()`. `file:` URIs are skipped for tiered scenarios. Class name is `test_create_excl` (filename is `test_excl.py`).
