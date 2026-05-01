# test_layered80 — Sweep server must not close ingest or layered dhandles during step-up or with pending truncate state

**File:** `test/suite/test_layered80.py`
**Storage mode:** Disagg/Layered
**Components under test:** Sweep server, ingest dhandle lifetime, layered dhandle protection, fast truncate state, `dh_sweeps` stat

## Test Cases

### `test_layered80.test_layered_dhandle_not_swept_during_stepup`
- **What it tests:** As follower, writes 1000 rows (ts=1000), pins the raw ingest dhandle by opening a cursor on `file:test_layered80.wt_ingest`, waits for the sweep server to complete at least 3 sweep cycles (polls `dh_sweeps` stat), then steps up to leader and closes the pinning cursor. Verifies all 1000 rows are present with no gaps after step-up. If the sweep server had incorrectly closed the layered dhandle before step-up, the drain operation during step-up would lose the ingest data.
- **Components:** `src/conn/conn_dhandle.c` (sweep), `src/conn/conn_layered_ingest.c` (step-up drain)
- **Notes:** Uses aggressive sweep config: `close_handle_minimum=0, close_idle_time=1, close_scan_interval=1, verbose=(sweep:3)`. Covers WT-16974 and WT-16703. The ingest dhandle itself is kept open to allow the sweep to run on the layered dhandle.

### `test_layered80.test_layered_dhandle_not_swept_with_truncate_state`
- **What it tests:** As follower, writes 1000 rows, then commits a truncate of rows 100–700 (ts=1001). Waits for the sweep server to complete at least 3 cycles while the truncate entry lives in the layered dhandle's in-memory truncate list. Verifies that after the sweep cycles, rows 100–700 are still absent (100 + 299 = 399 remaining rows). If the sweep had discarded the truncate entry by closing the dhandle, the truncated rows would reappear.
- **Components:** `src/conn/conn_dhandle.c` (sweep protection for truncate state), fast truncate implementation
- **Notes:** Skipped if `wiredtiger.disagg_fast_truncate_build() == 0`. Covers WT-16798. Uses the same aggressive sweep configuration.
