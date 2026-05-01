# test_hs21 — History store: idle file handle sweep does not lose active history or change write generation

**File:** `test/suite/test_hs21.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** history store, file handle sweep, timestamps, statistics (dh_sweep_dead_close, file_open)

## Test Cases

### `test_hs21.test_hs`
- **What it tests:** Creates 10 tables with 1,000 rows. Pins oldest/stable=1. Updates each table at ts=2 (first half of rows). Opens a long-running reader at ts=2. Updates each table at ts=100 (all rows). Advances stable=100. Waits up to 6 seconds (polling every 0.5 s) for the sweep server to close all 10 idle file handles, performing checkpoints to encourage sweeping. After sweep, verifies:
  1. At least 10 dhandles were closed (`dh_sweep_dead_close >= 10`).
  2. The long-running ts=2 reader still sees the correct value (first half of rows, value1).
  3. The most-recent ts=100 data is still readable.
  4. The `run_write_gen` for each file has not changed after re-opening (write generation increments only on restart, not on idle close/reopen).
- **Components:** `src/history/`, `src/conn/`, `src/session/`
- **Notes:** Scenarios: key_format ∈ {`r`, `S`}; `file_manager=(close_handle_minimum=0,close_idle_time=2,close_scan_interval=1)`. Skipped for tiered storage. The test validates a subtle correctness property: files with active history that are swept closed can be reopened without losing version data and without incrementing the write generation.
