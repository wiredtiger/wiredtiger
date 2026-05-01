# test_layered81 — Follower cursor visibility after checkpoint advance

**File:** `test/suite/test_layered81.py`
**Storage mode:** Disagg/Layered
**Components under test:** Follower checkpoint advance, cursor visibility of new/updated/deleted data, cursor bounds after advance, read timestamp interaction with checkpoint advance

## Test Cases

### `test_layered81.test_checkpoint_advance_full_scan`
- **What it tests:** Checkpoint 1: inserts 500 even keys on leader. Follower cursor does a full scan (sees even keys only) and resets. Checkpoint 2: inserts 500 odd keys on both leader and follower, advances checkpoint. Same cursor (after reset) does a full scan and verifies all 1000 keys appear in order.
- **Components:** `src/cursor/cur_layered.c`, follower checkpoint advance, `disagg_advance_checkpoint()`
- **Notes:** Uses zero-padded 6-digit string keys. The comment notes that in production the follower replicates leader operations before checkpoint pickup.

### `test_layered81.test_checkpoint_advance_updated_value`
- **What it tests:** Checkpoint 1: inserts 1000 keys with default values. Cursor searches key=0 and confirms value. Checkpoint 2: updates every 10th key with "updated_XXXXXX" values on both leader and follower, advances checkpoint. Verifies all updated keys have new values and non-updated keys retain original values.
- **Components:** Stable btree update visibility, ingest btree merge
- **Notes:** Covers both point-read (search) and the absence of stale values after checkpoint advance.

### `test_layered81.test_checkpoint_advance_deleted_key`
- **What it tests:** Checkpoint 1: 1000 keys. Scan confirms all present. Checkpoint 2: every 3rd key removed on leader, checkpoint advanced. Scan confirms only non-deleted keys remain; deleted keys return `WT_NOTFOUND` via `search()`.
- **Components:** Tombstone visibility after checkpoint advance
- **Notes:** Verifies both full-scan and point-search visibility of deleted keys.

### `test_layered81.test_checkpoint_advance_positioned_on_local_key`
- **What it tests:** Checkpoint 1: keys 0–499 on leader. Follower writes keys 500–999 locally. Cursor positioned on key 750. Checkpoint 2: adds key 1000 on both sides, advances. Without resetting, cursor searches key 1000 and verifies it is found.
- **Components:** Cursor checkpoint advance while positioned on local (ingest) key
- **Notes:** Tests that a cursor already positioned on an ingest key can transparently advance the checkpoint and see new stable data.

### `test_layered81.test_checkpoint_advance_interleaved`
- **What it tests:** Checkpoint 1: even keys 0–998 on leader. Follower writes odd keys 1–199 locally. Full scan confirms both sets in sorted order. Checkpoint 2: odd keys 1–999 on leader, checkpoint advanced. Scan confirms all 1000 keys present.
- **Components:** Merge of interleaved stable and ingest keys after checkpoint advance
- **Notes:** Tests that stable keys from the new checkpoint correctly merge with pre-existing ingest keys.

### `test_layered81.test_checkpoint_advance_search_near`
- **What it tests:** Checkpoint 1: all keys except 500. `search_near(500)` on follower returns a neighbor (499 or 501) with correct exact (-1 or +1). Checkpoint 2: adds key 500, advances. `search_near(500)` now returns exact=0 with key=500.
- **Components:** `search_near` after checkpoint advance, stable btree search
- **Notes:** Tests that `search_near` correctly re-targets the stable btree after a checkpoint advance.

### `test_layered81.test_checkpoint_advance_with_read_timestamp_iteration`
- **What it tests:** Checkpoint 1: keys 0–499 (records `ts_after_ckpt1`). Checkpoint 2: keys 500–999 (records `ts_after_ckpt2`). Transaction at `read_timestamp=ts_after_ckpt1` sees only 500 keys. Transaction at `read_timestamp=ts_after_ckpt2` sees all 1000 keys.
- **Components:** Read timestamp interaction with stable btree view, `layered_curs_advance_stable` implicit trigger
- **Notes:** Tests timestamp-gated visibility after two checkpoints are applied.

### `test_layered81.test_checkpoint_advance_preserves_bounds`
- **What it tests:** Inserts 1000 keys. Sets cursor bounds [200, 800]. Scans within bounds (601 keys). Resets, re-applies bounds. Adds two keys (1001, 1002) outside bounds, advances checkpoint. After reset and re-apply of bounds, scans again and confirms only keys 200–800 appear (1001/1002 excluded by bounds).
- **Components:** Cursor bounds enforcement after checkpoint advance, `src/cursor/cur_layered.c`
- **Notes:** Bounds are cleared by reset, so the test explicitly re-applies them.

### `test_layered81.test_checkpoint_advance_bounds_new_data_inside`
- **What it tests:** Checkpoint 1: even keys. Sets bounds [200, 800], scans (even keys in range). Checkpoint 2: odd keys added on both leader and follower. After re-applying bounds, scans and confirms all keys 200–800 (even and odd) appear.
- **Components:** Bounds + checkpoint advance with new data inside the bounded range
- **Notes:** Verifies that newly stable odd keys inside the bounds are visible after advance.

### `test_layered81.test_checkpoint_advance_tombstone_persists`
- **What it tests:** Inserts 1000 keys, checkpoints. Follower locally deletes keys 400–599. Scan confirms deleted range is absent. Checkpoint 2: adds leader keys 1000–1099, advances. Scan confirms: (1) deleted keys 400–599 remain hidden; (2) new keys 1000–1099 are now visible.
- **Components:** Ingest tombstone persistence across checkpoint advance
- **Notes:** Tests that locally-deleted ingest entries are not resurrected when a newer checkpoint arrives.

### `test_layered81.test_leader_unaffected_by_checkpoint`
- **What it tests:** Inserts keys 0–499 on leader, checkpoints. Leader cursor searches key=0 (found). Inserts keys 500–999, checkpoints. Leader cursor searches key=999 (found). Verifies that the leader's own cursor sees its writes without needing checkpoint advance.
- **Components:** Leader cursor self-visibility, not dependent on follower advance path
- **Notes:** Sanity check that the leader's cursor behavior is not accidentally broken by the follower-specific checkpoint advance logic.
