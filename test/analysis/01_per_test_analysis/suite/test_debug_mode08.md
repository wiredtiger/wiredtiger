# test_debug_mode08 — Tests debug_mode cursor_copy extra malloc/free on cursor operations

**File:** `test/suite/test_debug_mode08.py`
**Storage mode:** General
**Components under test:** debug mode, cursor copy, memory allocation

## Test Cases

### `test_debug_mode08` (inherits all tests from `test_base03`)
- **What it tests:** Runs the full `test_base03` test suite with `debug_mode=(cursor_copy=true)` enabled. The `cursor_copy` flag causes WiredTiger to copy cursor data into a fresh malloc on each `get_key`/`get_value`, then free it, to surface use-after-free bugs. All inherited tests pass under this regime.
- **Components:** `src/cursor/`, `src/conn/conn_debug.c`
- **Notes:** No new test logic is introduced; correctness is verified by the inherited suite. The value of the test is running existing cursor operations under the extra-malloc mode.

### `test_debug_mode08.test_reconfig`
- **What it tests:** Verifies the reconfigure lifecycle for `cursor_copy`: on -> off -> on again, with cursor activity (a single insert) at each step to confirm the flag changes take effect without crashing or corrupting data.
- **Components:** `src/cursor/`, `src/conn/conn_debug.c`
- **Notes:** Sequence: initial config has `cursor_copy=true`; reconfigure to `false`; reconfigure back to `true`.
