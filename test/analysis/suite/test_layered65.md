# test_layered65 — Garbage collection of prepared updates and rollbacks on the ingest table

**File:** `test/suite/test_layered65.py`
**Storage mode:** Disagg/Layered
**Components under test:** Ingest table garbage collection, prepared transaction eviction, `rec_ingest_garbage_collection_keys_update_chain`, `rec_ingest_garbage_collection_keys_disk_image`, `rec_ingest_keep_prepare_rollback`

## Test Cases

### `test_layered65.test_prepared_insert`
- **What it tests:** With a committed insert (key=1, ts=10) and a pending prepared insert (key=2, prepare_ts=20, stable=20), forces eviction of key=1 after a checkpoint. Verifies that exactly 1 entry is GC'd from the update chain (the committed insert) and the prepared insert is NOT collected while prepare is still active. Leaves the prepared transaction open (rolled back during teardown at ts=30).
- **Components:** `src/conn/conn_layered_ingest.c`, `src/btree/bt_evict.c`
- **Notes:** Parametrized by table_type (layered: vs table: with block_manager=disagg). Both leader and follower connections are open simultaneously and replicate each transaction. Eviction triggered via `debug=(release_evict_page)` on a second session.

### `test_layered65.test_prepared_insert_rollback`
- **What it tests:** Two-phase test. Phase 1: a prepared insert (key=2, ts=20) is rolled back at ts=30 on both leader and follower; after checkpoint at stable=20 and eviction, verifies GC count = 1 (only the committed key=1 is collected, not the rolled-back prepare). Phase 2: advances stable to 30 (rollback now stable), checkpoints again, re-evicts; verifies GC count becomes 2 (the aborted prepared insert is now also GC'd from the update chain).
- **Components:** `src/conn/conn_layered_ingest.c`, ingest GC logic
- **Notes:** Tests that aborted prepared inserts are deferred until the rollback timestamp becomes stable before being GC'd.

### `test_layered65.test_prepared_update`
- **What it tests:** Key=1 (ts=10) and key=2 (ts=10) are committed, stable=10, and evicted from the follower. Then key=2 is updated with a prepared transaction (prepare_ts=20, stable=20), checkpoint taken, follower advances, eviction triggered on key=1. Verifies `rec_ingest_garbage_collection_keys_update_chain=0` (nothing GC'd from update chain) and `rec_ingest_garbage_collection_keys_disk_image=2` (both keys GC'd from the disk image, because the prepared update blocks update-chain GC but disk-image GC of the pre-prepare value proceeds).
- **Components:** `src/conn/conn_layered_ingest.c`, disk image GC path
- **Notes:** Tests the distinction between update-chain GC and disk-image GC when a prepared update is present on an existing key.

### `test_layered65.test_prepared_update_rollback`
- **What it tests:** Same setup as `test_prepared_update` but the prepared update on key=2 is rolled back at ts=30. Phase 1 (stable=20): after checkpoint and eviction, verifies `update_chain=0` and `disk_image=2`. Phase 2 (stable=30): after another checkpoint and eviction, verifies `update_chain=1` (the rolled-back update is now GC'd) and `disk_image=2`.
- **Components:** `src/conn/conn_layered_ingest.c`
- **Notes:** Confirms the two-phase GC: disk-image GC is immediate once no prepare blocks it, but update-chain GC of the aborted prepare waits until its rollback_timestamp is stable.

### `test_layered65.test_prepared_insert_rollback_obsolete`
- **What it tests:** Three-phase test where a prepared insert is rolled back (ts=30) and then a new committed insert for the same key arrives (ts=40). On the follower, the committed update is set globally visible (oldest=40) while the leader's stable is only at 20. Phase 1 (leader stable=20, follower oldest=40): after eviction, verifies `garbage_collected=0` and `rec_ingest_keep_prepare_rollback=1` (aborted prepare must be retained because the rollback is not yet stable on the leader). Phase 2 (leader stable=30): after eviction, verifies `garbage_collected=0` still (committed update not yet stable) and `aborted_prepare_kept=1`. Phase 3 (leader stable=40): after eviction, verifies `rec_ingest_garbage_collection_keys_disk_image=1` (key finally GC'd) and `aborted_prepare_kept=1`.
- **Components:** `src/conn/conn_layered_ingest.c`
- **Notes:** Tests the interaction between an aborted prepare and a subsequent committed insert on the same key, verifying that GC is correctly deferred across all three phases.
