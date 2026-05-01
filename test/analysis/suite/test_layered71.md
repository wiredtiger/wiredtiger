# test_layered71 — Drop empty table during checkpoint, follower sees correct state

**File:** `test/suite/test_layered71.py`
**Storage mode:** Disagg/Layered
**Components under test:** Checkpoint + table drop race, sweep server, follower checkpoint advance, `dh_sweep_dead_close`

## Test Cases

### `test_layered71.test_layered71`
- **What it tests:** Starts as leader, creates an empty `table:` URI with `type=layered`. Waits for the sweep server to close the idle dhandle (polls `dh_sweep_dead_close`). Starts a slow checkpoint in a background thread (10 s delay via `timing_stress_for_test=[checkpoint_slow]`). Once the checkpoint is active, drops the empty table with `checkpoint_wait=false`. After the checkpoint thread completes: (1) opens a follower, advances checkpoint, verifies the table is still visible but empty (item_count=0); (2) runs a second checkpoint, advances follower, verifies `open_cursor()` on the dropped table now raises `WiredTigerError`.
- **Components:** `src/session/session_api.c` (checkpoint, drop), `src/conn/conn_dhandle.c` (sweep), `src/conn/conn_layered_ingest.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` and `threading.Thread`. The sweep config is aggressive: `close_handle_minimum=0, close_idle_time=1, close_scan_interval=1`. Tests that a checkpoint mid-drop does not corrupt the follower's view of the dropped table: the first checkpoint preserves the (empty) table; only after the drop is fully checkpointed does the follower observe the table as gone.
