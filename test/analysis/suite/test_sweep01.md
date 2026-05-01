# test_sweep01 — File handle sweep with concurrent checkpoints

**File:** `test/suite/test_sweep01.py`
**Storage mode:** General
**Components under test:** file manager sweep, dhandle lifecycle, checkpoint

## Test Cases

### `test_sweep01.test_ops`
- **What it tests:** Creates 30 tables and populates them; then keeps one table active while checkpointing, allowing the sweep server to close idle handles. Waits up to 60 seconds for `file_open` to drop to 5 (metadata + history store + lock + active + stats). Verifies that `dh_sweep_dead_close`, `dh_sweep_remove`, and `dh_sweeps` all increased and `file_open` decreased to the expected count.
- **Components:** `file_manager.c`, `dhandle.c`, `checkpoint.c`
- **Notes:** Skipped for disagg and tiered hooks. Parameterized over row and VLCS (`key_format=r`) table types. Configuration: `close_handle_minimum=0`, `close_idle_time=3`, `close_scan_interval=1`. Confirms sweep works correctly alongside active checkpointing (checkpoint's sweep of session cache allows dhandle removal).
