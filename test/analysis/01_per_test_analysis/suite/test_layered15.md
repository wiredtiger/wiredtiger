# test_layered15 — Restart without local files: metadata, data, and shared metadata validation

**File:** `test/suite/test_layered15.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** cold restart (no local files), metadata cursor, shared metadata (`WiredTigerShared.wt_stable`), checkpoint pickup, leader/follower roles, conn_layered.c

## Test Cases

### `test_layered15.test_layered15`
- **What it tests:** A two-restart scenario testing recovery from complete loss of local files.

  Setup: The node starts as follower, steps up to leader, creates four URIs (two layered via `layered:` prefix, one regular file with `block_manager=disagg`, one plain table with `block_manager=disagg`). Inserts 500 items into all URIs, checkpoints, and verifies shared metadata contains all URIs.

  **Restart 1:** Steps down to follower, calls `restart_without_local_files()` (moves all local `.wt` and `WiredTiger*` files to a save directory). Verifies the metadata cursor reports no URIs yet. Then picks up the last checkpoint via `conn.reconfigure(checkpoint_meta=...)`. Verifies shared metadata and local metadata are restored with all expected URIs, including ingest sub-files. Reads all data back correctly. Steps up as leader again. Performs selective updates (every 10th key in layered and table URIs), checkpoints, and re-verifies all data.

  **Restart 2:** Same process as restart 1, but also verifies the updated data is correct after recovery.

- **Components:** `restart_without_local_files` helper, checkpoint metadata pickup, shared metadata table (`WiredTigerShared.wt_stable`), local metadata reconstruction, ingest btree creation on pickup, conn_layered.c, palite page log
- **Notes:** Parametrized by disagg_storage scenario. Verifies: (1) metadata cursor is empty before checkpoint pickup, (2) shared metadata persists across local file loss, (3) all table types (layered:, table:+disagg, file:+disagg) are recoverable from page log alone, (4) ingest file URIs appear in metadata after pickup, (5) writes are possible after recovery. Logging is disabled on ingest/stable sub-files (`log=(enabled=false)`), which is verified by the `check_metadata_cursor` helper. Would break if checkpoint pickup fails to reconstruct metadata or if data is not accessible after losing local files.
