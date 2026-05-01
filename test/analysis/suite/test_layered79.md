# test_layered79 — On-disk ingest value removed after garbage collection during eviction

**File:** `test/suite/test_layered79.py`
**Storage mode:** Disagg/Layered
**Components under test:** Ingest btree GC on eviction, on-disk tombstone write, `rec_ingest_garbage_collection_keys_update_chain`

## Test Cases

### `test_layered79.test_updated_key_on_disk_value_removed_after_gc`
- **What it tests:** Five-step scenario: (1) inserts key=1 (ts=10) on both leader and follower; (2) force-evicts key=1 on follower so it has an on-disk image in the ingest btree; (3) updates key=1 to value2 (ts=20) on both sides, checkpoints (stable=20), follower advances; (4) force-evicts key=1 again on follower (triggering GC — the update is now prunable because it's stable and in the stable btree); (5) directly opens the ingest URI (`file:test_layered79.wt_ingest`) and verifies key=1 is `WT_NOTFOUND`, and checks `rec_ingest_garbage_collection_keys_update_chain=1`.
- **Components:** `src/conn/conn_layered_ingest.c`, ingest GC path, on-disk tombstone write
- **Notes:** The key invariant is that when an ingest entry is GC'd (because it moved to the stable btree via checkpoint), the old on-disk image in the ingest btree must also be tombstoned — otherwise a future cold read of the ingest btree would silently return a stale value.

### `test_layered79.test_deleted_key_on_disk_value_removed_after_gc`
- **What it tests:** Same five-step pattern as `test_updated_key_on_disk_value_removed_after_gc` but step 3 is a removal (tombstone) instead of an update. After eviction/GC, verifies key=1 is `WT_NOTFOUND` in the ingest btree and `rec_ingest_garbage_collection_keys_update_chain=1`.
- **Components:** `src/conn/conn_layered_ingest.c`, ingest GC path for tombstones
- **Notes:** Tests that a delete (not an update) also correctly tombstones the on-disk ingest value during GC.

### `test_layered79.test_global_visible_tombstone_clears_update_chain_and_on_disk_value`
- **What it tests:** Inserts key=1 (ts=10) and evicts it to disk. Then directly removes key=1 from the raw ingest btree URI (`file:test_layered79.wt_ingest`) using `no_timestamp=true` (globally visible tombstone). Sets stable=1 (lower than the commit ts=10 so the on-disk ingest value is not yet prunable by timestamp), checkpoints, follower advances, evicts again. Verifies key=1 is `WT_NOTFOUND` in the ingest btree and `rec_ingest_garbage_collection_keys_update_chain=1`.
- **Components:** `src/conn/conn_layered_ingest.c`, ingest GC with globally visible tombstone
- **Notes:** Tests the case where a globally visible (no-timestamp) tombstone on the ingest btree must still clear the existing on-disk entry even though the timestamp-based pruning would not yet apply.
