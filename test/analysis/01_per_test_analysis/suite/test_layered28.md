# test_layered28 — Drop layered tables: metadata cleanup, shared metadata, follower drop semantics

**File:** `test/suite/test_layered28.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** table drop (schema ops), metadata cleanup, shared metadata (`WiredTigerShared.wt_stable`), sweep thread, follower drop behavior, conn_layered.c

## Test Cases

### `test_layered28.test_create_drop`
- **What it tests:** Creates a layered table (either `layered:` or `table:` with disagg config), inserts 1000 records, checkpoints, then drops the table. Calls `validate_drop()` which verifies: (1) `metadata:` cursor does not find any of the sub-URIs (`*.wt_stable`, `*.wt_ingest`, `layered:`, `table:`, `colgroup:`), (2) opening a cursor on the dropped URI raises `WiredTigerError`. Then checkpoints again (to persist the drop to shared metadata) and verifies the shared metadata table `WiredTigerShared.wt_stable` no longer contains the table entries.
- **Components:** `session.drop()` on layered table, metadata cleanup for all sub-URIs, shared metadata persistence via checkpoint, conn_layered.c
- **Notes:** Parametrized across 2 table types (layered: prefix, table:+disagg+type=layered) and disagg_storage. The two-checkpoint approach (drop then checkpoint) is needed to persist the schema change to shared metadata.

### `test_layered28.test_create_drop_checkpoint`
- **What it tests:** Same as `test_create_drop` but uses a separate `custom_session` for create/drop to allow the session to be closed before checkpointing, releasing the dhandle reference. This tests that the sweep thread can close out the dhandle without crashing after a drop.
- **Components:** dhandle lifecycle after drop, sweep thread interaction with dropped layered handles, session close after drop
- **Notes:** The `file_manager=(close_scan_interval=1)` config on the connection ensures the sweep thread runs frequently. Tests a specific race condition where a handle is dropped but not yet fully closed when the sweep thread runs.

### `test_layered28.test_create_drop_follower`
- **What it tests:** Creates a layered table as leader, inserts 1000 records, checkpoints, captures checkpoint metadata. Reopens connection as follower (with the checkpoint meta). Drops the table on the follower. Validates the drop (local metadata cleaned up, cursor raises error). Checkpoints on the follower (to persist the drop operation locally) and then checks that the shared metadata still has the table entries (because a follower drop should not remove entries from shared metadata — only a leader drop does).
- **Components:** follower drop semantics, local vs shared metadata after drop, conn_layered.c
- **Notes:** The critical assertion is `check_shared_metadata(expect_exists=True)` after the follower drop — this confirms that a follower-local drop does not propagate to the shared metadata table, preserving the leader's view of the schema.
