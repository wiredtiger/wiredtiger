# test_debug_mode02 — Tests debug_mode checkpoint_retention log file pinning

**File:** `test/suite/test_debug_mode02.py`
**Storage mode:** General
**Components under test:** debug mode, logging, checkpoint, log file retention

## Test Cases

### `test_debug_mode02.test_checkpoint_retain`
- **What it tests:** Verifies that with `checkpoint_retention=5`, log files are not removed while fewer than `retain` (5) checkpointed log files exist, and that the first log file is eventually removed only after the retention threshold is exceeded. Also validates that reconfiguring `table_logging=true` and `verbose=(temporary)` does not crash.
- **Components:** `src/log/`, `src/checkpoint/`, `src/conn/conn_debug.c`
- **Notes:** Uses `file_max=100K` to force log rotation quickly. Loops until log file set grows; then waits up to 90 seconds for file removal. Validates log set is a proper superset after each checkpoint advance.

### `test_debug_mode02.test_checkpoint_retain_reconfig`
- **What it tests:** Validates the full reconfigure lifecycle for `checkpoint_retention`: turning it off (0), on (5), same value again (5), attempting a change to a different non-zero value (expected error "Cannot change value"), off again, and back on to the original value.
- **Components:** `src/log/`, `src/checkpoint/`, `src/conn/conn_debug.c`
- **Notes:** Confirms that changing retention to a different non-zero value raises `WiredTigerError`. Confirms that log files are removed when retention is 0. Tests the sequence: 0 -> 5 -> 5 -> (4 fails) -> 0 -> 5.
