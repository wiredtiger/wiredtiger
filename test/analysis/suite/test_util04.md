# test_util04 — wt drop CLI: table removal

**File:** `test/suite/test_util04.py`
**Storage mode:** General
**Components under test:** `wt drop`, schema drop

## Test Cases

### `test_util04.test_drop_process`
- **What it tests:** Creates a table; runs `wt drop table:<name>`; verifies the table no longer exists on disk (`tableExists` returns false) and that attempting to open a cursor raises `WiredTigerError`.
- **Components:** `util_drop.c`, `schema.c`
- **Notes:** No parameterization. Simple smoke test for the `wt drop` CLI subcommand.
