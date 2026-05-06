# test_sweep03 — Sweep disabled by close_idle_time=0; drop-triggered handle close

**File:** `test/suite/test_sweep03.py`
**Storage mode:** General
**Components under test:** file manager sweep, dhandle close, table drop, cache reclamation

## Test Cases

### `test_sweep03.test_disable_idle_timeout1`
- **What it tests:** Creates 40 tables (more than `close_handle_minimum=10`), waits for the sweep server to run twice, then verifies `dh_sweep_dead_close == 0`, confirming that `close_idle_time=0` disables idle-based handle sweeping.
- **Components:** `file_manager.c`, `dhandle.c`
- **Notes:** Parameterized over row and VLCS table types.

### `test_sweep03.test_disable_idle_timeout_drop_force`
- **What it tests:** Creates a table, populates it, records cache and close stats, force-drops the table (`force=true`), waits for sweep, then verifies `dh_sweep_dead_close` increased (1 for non-disagg, 2 for disagg row since both stable and ingest files are dropped) and `cache_bytes_inuse` decreased.
- **Components:** `file_manager.c`, `dhandle.c`, `schema.c`, `cache.c`
- **Notes:** Skipped for tiered. Disagg row mode closes 2 handles (stable + ingest). Parameterized.

### `test_sweep03.test_disable_idle_timeout_drop`
- **What it tests:** Creates a table, populates it, performs a regular (non-force) drop, waits for sweep, and verifies `dh_sweep_dead_close` did not change (regular drop does not use sweep) while cache bytes decreased.
- **Components:** `file_manager.c`, `dhandle.c`, `schema.c`
- **Notes:** Skipped for disagg (FIXME-WT-16757) and tiered. Confirms sweep server is not involved in regular drop cleanup.
