# test_home — Connection home directory, is_new, and base configuration file

**File:** `test/suite/test_home.py`
**Storage mode:** General
**Components under test:** connection API (is_new, get_home), configuration (config_base)

## Test Cases

### `test_isnew.test_isnew`
- **What it tests:** Verifies that `conn.is_new()` returns `True` after the database is first created, and `False` after closing and reopening an existing database.
- **Components:** `src/conn/`

### `test_gethome.test_gethome_default`
- **What it tests:** Verifies that `conn.get_home()` returns `'.'` for a connection opened with the default home directory.
- **Components:** `src/conn/`

### `test_gethome.test_gethome_new`
- **What it tests:** Creates a new database directory, opens a connection in it, and verifies `conn.get_home()` returns the directory name.
- **Components:** `src/conn/`

### `test_base_config.test_base_config`
- **What it tests:** Verifies that a `WiredTiger.basecfg` file is created in the default home when a new database is opened. Then opens a second database with `config_base=false` and verifies that `WiredTiger.basecfg` is not created in that directory.
- **Components:** `src/conn/`, `src/config/`
- **Notes:** Three independent test classes in one file: `test_isnew`, `test_gethome`, `test_base_config`. No scenarios.
