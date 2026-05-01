# test_config07 — Log file pre-allocation (file_extend) configuration

**File:** `test/suite/test_config07.py`
**Storage mode:** General
**Components under test:** log subsystem, file_extend config

## Test Cases

### `test_config07.test_log_extend`
- **What it tests:** `file_extend=(log=<size>)` config: validates accepted sizes (default, empty, disabled, 100K, too_small, too_large, small_in_allowed_range, large_in_allowed_range, larger_than_log_file_size) and `data` file extend combined config.
- **Components:** `src/log/`, `src/config/`
- **Notes:** Scenarios include: `default` (no config), `empty` (empty string), `disable` (0), `100K`, `too_small` (below minimum), `too_large` (above maximum), `small_in_allowed_range`, `large_in_allowed_range`, `larger_than_log_file_size`, and `with_data_file_extend_conf`. Expects errors for `too_small` and `too_large`. Verifies log files are pre-allocated to the configured size.
