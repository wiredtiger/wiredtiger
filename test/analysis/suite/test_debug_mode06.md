# test_debug_mode06 — Tests debug_mode slow_checkpoint artificial delay injection

**File:** `test/suite/test_debug_mode06.py`
**Storage mode:** General
**Components under test:** debug mode, checkpoint, statistics

## Test Cases

### `test_debug_mode06.test_slow_checkpoints`
- **What it tests:** Verifies that with `debug_mode=(slow_checkpoint=true)` the checkpoint duration (as reported by the `checkpoint_time_recent` statistic) is at least 10 ms, confirming the artificial delay is actually applied.
- **Components:** `src/checkpoint/`, `src/conn/conn_debug.c`, `src/support/stat.c`
- **Notes:** Uses `statistics=(all)` to enable the checkpoint timing stat. The test avoids absolute timing assertions where possible but does assert a 10 ms lower bound.

### `test_debug_mode06.test_slow_checkpoints_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(slow_checkpoint=false)` disables the artificial delay. Checkpoint runs without timing assertion, confirming no crash and that normal operation is restored.
- **Components:** `src/checkpoint/`, `src/conn/conn_debug.c`
- **Notes:** No minimum time assertion is made when the flag is off, since the checkpoint might be fast for any reason.
