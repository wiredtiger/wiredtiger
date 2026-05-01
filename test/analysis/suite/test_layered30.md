# test_layered30 — Empty table creation and recovery from page log (with optional second table)

**File:** `test/suite/test_layered30.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** empty table checkpoint and recovery, checkpoint propagation to follower, restart without local files, conn_layered.c, page log (palite)

## Test Cases

### `test_layered30.test_layered30`
- **What it tests:** A two-part empty-table recovery test:

  **Setup:** Starts as follower, steps up to leader. Optionally creates a second table with one data record (the `another_table=True` scenario exercises the case where only one table in the checkpoint is empty). Creates the primary empty table. Checkpoints.

  **Part 1 (follower picks up checkpoint):** Opens a new follower connection and advances it to the checkpoint. Verifies the empty table exists on the follower (cursor opens successfully) and returns 0 records. Closes the follower.

  **Part 2 (restart without local files):** Calls `restart_without_local_files(step_up=True)` to simulate a cold start. Verifies the empty table is accessible and returns 0 records after recovery.

- **Components:** empty table checkpoint handling, checkpoint propagation for tables with no pages, `restart_without_local_files`, follower checkpoint pickup, conn_layered.c, palite page log
- **Notes:** Parametrized across 2 table types (layered: prefix, table:+disagg+type=layered) x 2 data scenarios (one-table empty, two-tables where one has data) x disagg_storage. Uses `precise_checkpoint=true`. The `another_table=True` scenario is specifically designed to exercise the case where a checkpoint includes a mix of populated and empty tables — ensuring the page log correctly records (or skips) empty-table entries. Would break if empty tables are not registered in the checkpoint, or if the follower or cold-restart node cannot open a table that had no pages written.
