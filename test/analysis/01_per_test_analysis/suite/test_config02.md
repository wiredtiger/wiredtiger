# test_config02 — wiredtiger_open home directory resolution

**File:** `test/suite/test_config02.py`
**Storage mode:** General
**Components under test:** connection API (`wiredtiger_open`), home directory, environment variables

## Test Cases

### `test_config02.test_home_nohome`
- **What it tests:** Opening a connection without specifying a home directory; uses current directory.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_rel`
- **What it tests:** Opening a connection with a relative home directory path.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_abs`
- **What it tests:** Opening a connection with an absolute home directory path.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_and_env`
- **What it tests:** Home path specified both via argument and via `WIREDTIGER_HOME` env var; argument takes precedence.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_and_env_conf`
- **What it tests:** Home path combined with `WIREDTIGER_CONFIG` env var.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_and_missing_env`
- **What it tests:** Home specified but `WIREDTIGER_HOME` env var is absent; verifies fallback behavior.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_env_conf`
- **What it tests:** Home directory resolved from `WIREDTIGER_HOME` env var only.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_env_conf_without_env_var`
- **What it tests:** `WIREDTIGER_CONFIG` env var used without `WIREDTIGER_HOME`; expects error or fallback.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_does_not_exist`
- **What it tests:** Opening with a home directory that doesn't exist; expects an error.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.

### `test_config02.test_home_not_writeable`
- **What it tests:** Opening with a home directory that exists but is not writeable; expects an error.
- **Components:** `src/conn/conn_open.c`
- **Notes:** Skipped for tiered hook.
