# test_config12 — debug_mode=(configuration) verbose config validation warnings

**File:** `test/suite/test_config12.py`
**Storage mode:** General
**Components under test:** connection config, debug_mode, config validation warnings

## Test Cases

### `test_config12.test_config12`
- **What it tests:** With `debug_mode=(configuration=true)`, opening a connection with eviction parameters that are technically valid but potentially misconfigured emits a warning; with `configuration=false` no warning is emitted.
- **Components:** `src/conn/conn_open.c`, `src/config/`

### `test_config12.test_config12_check1`
- **What it tests:** Specific eviction param relationship check 1 with debug_mode configuration enabled.
- **Components:** `src/config/`, `src/evict/`

### `test_config12.test_config12_check2`
- **What it tests:** Specific eviction param relationship check 2.
- **Components:** `src/config/`, `src/evict/`

### `test_config12.test_config12_check3`
- **What it tests:** Specific eviction param relationship check 3.
- **Components:** `src/config/`, `src/evict/`
