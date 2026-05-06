# test_layered75 — Metadata file ID correctness and namespace assignment

**File:** `test/suite/test_layered75.py`
**Storage mode:** Disagg/Layered
**Components under test:** File ID assignment, namespace bits (local/shared/special), metadata integrity, `WiredTigerShared.wt_stable`, `WiredTigerSharedHS.wt_stable`

## Test Cases

### `test_layered75.test_empty_database`
- **What it tests:** On an empty database (no user tables), verifies the metadata contains only the expected fixed-ID entries (`metadata:` has ID 0, `file:WiredTigerShared.wt_stable` has namespaced ID 10 = (1<<3)|2_special, `file:WiredTigerSharedHS.wt_stable` has namespaced ID 18 = (2<<3)|2_special, `file:WiredTigerHS.wt` in local namespace). Runs `check_metadata_ids()` on both leader session and a fresh follower session.
- **Components:** `src/conn/conn_disagg.c`, metadata subsystem, PALite namespace management
- **Notes:** Validates the fixed-ID invariant: changing these IDs would break backward compatibility. Parametrized by creation_format (layered-bare / layered-disagg / table-type-layered); the format does not affect an empty database.

### `test_layered75.test_populate_table_on_leader`
- **What it tests:** Creates one layered table on the leader, then verifies `file:test_layered75.wt_stable` is in the shared namespace and `file:test_layered75.wt_ingest` is in the local namespace. Also checks the follower's metadata (before any checkpoint advance) contains only the baseline entries.
- **Components:** `src/conn/conn_disagg.c`, file ID allocation per namespace
- **Notes:** Tests that the two physical files backing a single layered table are assigned to the correct namespaces at creation time.

### `test_layered75.test_populate_table_on_leader_pick_up_on_follower`
- **What it tests:** Creates one layered table, checkpoints, advances the follower, then verifies both leader and follower metadata contain `wt_stable` (shared) and `wt_ingest` (local) with correct namespace bits.
- **Components:** Checkpoint metadata propagation, follower namespace assignment
- **Notes:** Verifies that after checkpoint pickup the follower also sees the stable file with a shared namespace ID.

### `test_layered75.test_populate_10_tables_on_leader_pick_up_on_follower`
- **What it tests:** Creates 10 layered tables, checkpoints, advances the follower, then verifies that all 20 physical files (10 × `wt_stable` + 10 × `wt_ingest`) have correct namespaces and no duplicated IDs within each namespace. Also checks incremental ID assignment within each namespace (no gaps up to max).
- **Components:** File ID counter, namespace ID deduplication
- **Notes:** Tests that IDs are assigned incrementally without collisions across multiple tables.

### `test_layered75.test_populate_10_tables_on_leader_and_follower`
- **What it tests:** Creates 10 tables on the leader, checkpoints, advances the follower, then calls `session.create()` for the same 10 tables again on the follower (reuse path). Verifies that the follower's ingest tables land in the local namespace and the stable tables reuse the shared namespace IDs from the leader's checkpoint.
- **Components:** Ingest dhandle creation on follower, namespace reuse vs allocation
- **Notes:** Tests the case where a follower opens an already-checkpointed layered table and must not allocate a new shared-namespace ID for the stable btree.
