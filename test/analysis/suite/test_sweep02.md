# test_sweep02 — Sweep server configuration options

**File:** `test/suite/test_sweep02.py`
**Storage mode:** General
**Components under test:** file manager configuration, sweep server

## Test Cases

### `test_sweep02.test_config01`
- **What it tests:** Opens a connection with `file_manager=()` (empty config) — verifies it succeeds.
- **Components:** `file_manager.c`

### `test_sweep02.test_config02`
- **What it tests:** Opens with `file_manager=(close_scan_interval=1)` — verifies it succeeds.
- **Components:** `file_manager.c`

### `test_sweep02.test_config03`
- **What it tests:** Opens with `file_manager=(close_idle_time=1)` — verifies it succeeds.
- **Components:** `file_manager.c`

### `test_sweep02.test_config04`
- **What it tests:** Opens with `file_manager=(close_handle_minimum=500)` — verifies it succeeds.
- **Components:** `file_manager.c`

### `test_sweep02.test_config05`
- **What it tests:** Opens with `file_manager=(close_scan_interval=1,close_idle_time=1)` — verifies the combination succeeds.
- **Components:** `file_manager.c`
- **Notes:** All tests use manual connection setup; they only test that configuration parsing succeeds without error.
