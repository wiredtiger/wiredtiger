# test_env01 — WIREDTIGER_HOME environment variable and privileged access

**File:** `test/suite/test_env01.py`
**Storage mode:** General (skipped for tiered storage; Unix only)
**Components under test:** connection open, environment configuration

## Test Cases

### `test_priv01.test_home_and_env_conf_priv`
- **What it tests:** Opens with an explicit home directory and `WIREDTIGER_HOME` set plus `use_environment_priv=true`. Verifies that the explicit home directory wins over the environment variable.
- **Components:** `src/conn/`
- **Notes:** Unix-only (skipped on Windows). Skipped for tiered storage hook.

### `test_priv01.test_home_and_missing_env_priv`
- **What it tests:** Opens with an explicit home directory and no `WIREDTIGER_HOME` set. Verifies the explicit home directory is used.
- **Components:** `src/conn/`
- **Notes:** Unix-only.

### `test_priv01.test_env_conf_nopriv`
- **What it tests:** When running as a privileged user (euid != uid) without `use_environment_priv`, opening with `WIREDTIGER_HOME` set should raise an error about lacking privileges to use the environment variable.
- **Components:** `src/conn/`
- **Notes:** Only performs the assertion if `os.getuid() != os.geteuid()` (i.e., running setuid).

### `test_priv01.test_env_conf_priv`
- **What it tests:** When running as a privileged user with `use_environment_priv=true` and `WIREDTIGER_HOME` set, verifies that the environment home is used and the current directory receives no files.
- **Components:** `src/conn/`
- **Notes:** Only asserts when running setuid.

### `test_priv01.test_env_conf_without_env_var_priv`
- **What it tests:** With `use_environment_priv=true` but no `WIREDTIGER_HOME` set, verifies that the current directory is used as the home.
- **Components:** `src/conn/`

### `test_priv01.test_env_conf`
- **What it tests:** With `use_environment=true` and `WIREDTIGER_HOME` set, verifies that `wiredtiger_open(NULL, ...)` uses the environment-specified directory.
- **Components:** `src/conn/`

### `test_priv01.test_env_conf_off`
- **What it tests:** With `use_environment=false` and `WIREDTIGER_HOME` set, verifies that the environment variable is ignored and the current directory is used.
- **Components:** `src/conn/`
- **Notes:** Actual class name in the file is `test_priv01` (the file is `test_env01.py` by filename convention). The test covers all non-default combinations of `use_environment` and `use_environment_priv` config options.
