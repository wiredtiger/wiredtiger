# test_tiered08 — Concurrent inserts with background checkpoint and flush_tier threads

**File:** `test/suite/test_tiered08.py`
**Storage mode:** Tiered
**Components under test:** concurrent checkpoint/flush_tier under insert load, `timing_stress_for_test=(tiered_flush_finish)`, background flush thread (`wtthread.flush_checkpoint_thread`), data integrity after concurrent operations and connection reopen

## Test Cases

### `test_tiered08.test_tiered08`
- **What it tests:** Stress test that runs a background thread performing checkpoint and flush_tier operations (flush on approximately 1 in 4 checkpoints) concurrently with a main thread that inserts batches of 100 000 key-value pairs. The test continues inserting until at least 200 checkpoints and 50 flush_tier operations have been counted via statistics. After the background thread finishes, all inserted records are spot-checked (every 237th key) against their expected values. The connection is then closed and reopened, and the same verification is repeated to confirm durability across restart.
- **Components:** `src/tiered/conn_tiered.c`, flush_tier path, tiered manager background thread, `timing_stress_for_test` (tiered_flush_finish adds a 1 s delay to `flush_finish` to increase concurrency exposure), `src/session/session_api.c` checkpoint, statistics (`stat.conn.checkpoints_api`, `stat.conn.flush_tier`)
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store).
  - Table uses `internal_page_max=4096,leaf_page_max=4096` for small pages to increase flush frequency.
  - Value size varies with key index (modulo-12 repetition of `'filler'`) to produce varied data.
  - `ignoreStdoutPattern` suppresses expected "oldest id pinned in session" messages.
  - Tags: `tiered_storage:checkpoint`, `tiered_storage:flush_tier`.
