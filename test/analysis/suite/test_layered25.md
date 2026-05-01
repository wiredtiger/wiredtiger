# test_layered25 — Historical reads after restart without local files (history store validation)

**File:** `test/suite/test_layered25.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** historical reads (history store), restart without local files, checkpoint pickup, timestamped reads, conn_layered.c, page log (palite)

## Test Cases

### `test_layered25.test_layered25`
- **What it tests:** A two-part restart test verifying that historical timestamped reads work after recovery.

  **Setup:** Starts as follower, steps up to leader. Creates a table (parametrized type). Inserts 500 records at timestamp 100, checkpoints. Updates all 500 records at timestamp 200, sets oldest_timestamp=1, stable_timestamp=200, checkpoints. Verifies both timestamps work: ts=100 returns original values, ts=200 returns updated values.

  **Part 1 (normal reopen, keep local files):** Reopens the connection with checkpoint metadata, steps back up to leader. Sets stable_timestamp=200 (needed for precise_checkpoint). Reads at ts=200 (should see updated values) and ts=100 (uses history store — verifies old values are accessible from the history store after reopen).

  **Part 2 (restart without local files):** Calls `restart_without_local_files(step_up=True)` which moves all local files away, reopens cleanly from the page log, picks up the last checkpoint, and promotes to leader. Sets stable_timestamp=200. Again verifies both ts=200 and ts=100 — the ts=100 read still requires the history store, which must have been rebuilt from the page log.

- **Components:** history store reconstruction from page log, `restart_without_local_files`, precise checkpoint, historical timestamped reads, conn_layered.c, palite page log
- **Notes:** Parametrized across 3 table types (layered: prefix, table:+disagg+type=layered, table:+disagg+log=disabled) and disagg_storage. Uses `precise_checkpoint=true`. The critical assertion is that historical reads (ts=100) still work after `restart_without_local_files` — this means the history store must be durably stored in the page log and reconstructed on restart. Would break if the history store is not written to the page log or if it is not accessible after losing local files.
