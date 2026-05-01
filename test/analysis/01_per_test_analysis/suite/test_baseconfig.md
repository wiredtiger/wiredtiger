# test_baseconfig — Invalid WiredTiger.basecfg is rejected; config_base=false overrides it

**File:** `test/suite/test_baseconfig.py`
**Storage mode:** General
**Components under test:** connection open, base configuration file parsing

## Test Cases

### `test_baseconfig.test_baseconfig`
- **What it tests:** Opens a secondary database, marks it as corrupted (to create a `WiredTiger.basecfg`), appends invalid content (`foo!`) to the basecfg file, then closes the connection. Verifies that reopening the database without `config_base=false` raises `WiredTigerError` with `/unknown configuration key/`. Then verifies that reopening with `create,config_base=false` succeeds (the basecfg is ignored).
- **Components:** `src/conn/conn_open.c`, `src/config/config_api.c`
- **Notes:** Non-parametrized. Demonstrates the basecfg validation and the `config_base=false` escape hatch.
