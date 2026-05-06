# test_layered05 — Layered cursor search_near edge cases across stable and ingest btrees

**File:** `test/suite/test_layered05.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** cursor search_near, cursor bounds, layered cursor merging of stable and ingest btrees, tombstone handling, cur_layered.c

## Test Cases

### `test_layered05.test_search_near_empty`
- **What it tests:** Calls `search_near` on an empty table (no data, no checkpoint) and verifies `WT_NOTFOUND`. Then takes an empty checkpoint and repeats the check.
- **Components:** `cur_layered.c`, ingest btree, page log (palite)
- **Notes:** Both empty-ingest-only and empty-stable-after-checkpoint cases must return `WT_NOTFOUND`.

### `test_layered05.test_search_near_ingest_only`
- **What it tests:** Inserts odd keys 1, 3, ..., 999 only into the ingest (follower-local) btree with no checkpoint. Tests exact match, key-before-all, key-after-all, and key-between-two-odd-keys scenarios for `search_near`.
- **Components:** `cur_layered.c`, ingest btree
- **Notes:** No stable data exists. Verifies that `search_near` on the ingest-only path correctly identifies neighbors. Either adjacent neighbor is a valid result for between-two-keys cases.

### `test_layered05.test_search_near_stable_only`
- **What it tests:** Inserts even keys 0, 2, ..., 998 into the stable btree via leader checkpoint. Tests exact matches, key-before-all (empty string), key-after-all, and key-between-two-existing-keys on the follower.
- **Components:** `cur_layered.c`, stable btree, checkpoint, page log (palite)
- **Notes:** Only stable data (no local ingest writes). Verifies follower reads from stable btree for `search_near`.

### `test_layered05.test_search_near_split_data`
- **What it tests:** Even keys 0, 2, ..., 998 are checkpointed (stable), odd keys 1, 3, ..., 999 are written locally (ingest). Tests exact match on odd/even keys, boundary searches, and verifies forward/backward iteration is sorted after positioning with `search_near`.
- **Components:** `cur_layered.c`, stable btree, ingest btree
- **Notes:** The core case for the layered merge cursor: results must be correctly merged from both btrees in sorted order.

### `test_layered05.test_search_near_opposite_sides`
- **What it tests:** Keys 0-499 are in stable, keys 500-999 are in ingest. Tests exact match at boundary (500), between-two-keys at the boundary (key "000499x"), and verifies forward iteration is sorted after positioning.
- **Components:** `cur_layered.c`, stable btree, ingest btree
- **Notes:** Exercises the stable/ingest boundary specifically.

### `test_layered05.test_search_near_opposite_sides_farther`
- **What it tests:** Stable has key 498, ingest has key 900. `search_near(500)`: both neighbors are far from 500 and on opposite sides — either 498 or 900 is a valid result.
- **Components:** `cur_layered.c`, stable btree, ingest btree
- **Notes:** Tests that `search_near` handles large gaps between neighbors correctly.

### `test_layered05.test_search_near_neighbors_local_on_both_sides`
- **What it tests:** Stable has key 200, ingest has keys 300 and 900. `search_near(500)`: neighbors 300 (below, from ingest) and 900 (above, from ingest); either is valid.
- **Components:** `cur_layered.c`, stable btree, ingest btree

### `test_layered05.test_search_near_neighbors_lower_from_checkpoint`
- **What it tests:** Stable has key 200, ingest has keys 100 and 900. `search_near(500)`: lower neighbor 200 (from stable), upper neighbor 900 (from ingest).
- **Components:** `cur_layered.c`, stable btree, ingest btree

### `test_layered05.test_search_near_neighbors_nearest_upper_is_local`
- **What it tests:** Stable has key 200, ingest has keys 300, 600, 900. `search_near(500)`: nearest lower 300 (from ingest), nearest upper 600 (from ingest).
- **Components:** `cur_layered.c`, ingest btree

### `test_layered05.test_search_near_neighbors_upper_from_checkpoint`
- **What it tests:** Stable has key 800, ingest has keys 100 and 300. `search_near(500)`: lower 300 (from ingest), upper 800 (from stable).
- **Components:** `cur_layered.c`, stable btree, ingest btree

### `test_layered05.test_search_near_neighbors_nearest_lower_is_local`
- **What it tests:** Stable has key 800, ingest has keys 100, 300, 600. `search_near(500)`: nearest lower 300 (from ingest), nearest upper 600 (from ingest).
- **Components:** `cur_layered.c`, ingest btree

### `test_layered05.test_search_near_both_larger`
- **What it tests:** Stable has key 800, ingest has key 600. Both are above 500. `search_near(500)` must return 600 (the nearest above).
- **Components:** `cur_layered.c`, stable btree, ingest btree
- **Notes:** Tests the case where all live keys are above the search key; result must be the one with exact=1.

### `test_layered05.test_search_near_both_smaller`
- **What it tests:** Stable has key 100, ingest has key 400. Both below 500. `search_near(500)` must return 400 (nearest below).
- **Components:** `cur_layered.c`, stable btree, ingest btree

### `test_layered05.test_search_near_ingest_exact_deleted`
- **What it tests:** Stable has keys 200, 500, 700. Ingest deletes key 500 (tombstone). `search_near(500)` must not return the deleted key — either 200 or 700 is valid.
- **Components:** `cur_layered.c`, stable btree, ingest btree, tombstone resolution
- **Notes:** Critical correctness check: the layered cursor must recognize ingest tombstones that shadow stable data.

### `test_layered05.test_search_near_ingest_exact_deleted_walk_backward`
- **What it tests:** Stable has keys 200 and 500. Ingest deletes 500. Only smaller neighbor 200 remains. `search_near(500)` must return 200 with exact=-1.
- **Components:** `cur_layered.c`, tombstone resolution

### `test_layered05.test_search_near_ingest_exact_deleted_stable_no_match`
- **What it tests:** Stable has keys 300 and 700. Ingest inserts then deletes 500. `search_near(500)`: deleted key has no value, equidistant neighbors 300 and 700.
- **Components:** `cur_layered.c`, ingest btree, tombstone resolution

### `test_layered05.test_search_near_ingest_exact_deleted_all_tombstoned`
- **What it tests:** Stable has keys 300, 500, 700. Ingest deletes all three. `search_near(500)` must return `WT_NOTFOUND`.
- **Components:** `cur_layered.c`, tombstone resolution

### `test_layered05.test_search_near_all_deleted`
- **What it tests:** All 1000 keys are in stable. Ingest deletes all 1000 keys. `search_near(500)` must return `WT_NOTFOUND`.
- **Components:** `cur_layered.c`, tombstone resolution at scale

### `test_layered05.test_search_near_tombstone_cross_table`
- **What it tests:** Stable has keys 200 and 700. Ingest re-inserts 200 and deletes 700. `search_near(500)` must return 200 (the only live key).
- **Components:** `cur_layered.c`, stable btree, ingest btree, tombstone resolution

### `test_layered05.test_search_near_then_iterate`
- **What it tests:** Even keys 0-998 in stable, odd keys 1-999 in ingest. `search_near(500)` finds exact match, then iterates forward (501, 502, 503) and backward (499, 498, 497). Verifies that post-`search_near` iteration correctly merges both btrees in order.
- **Components:** `cur_layered.c`, cursor iteration after search_near

### `test_layered05.test_search_near_tombstone_then_iterate`
- **What it tests:** All 1000 keys in stable. Ingest deletes key 500. `search_near(500)` lands on either 499 or 501. Then iterates: if on 501, `prev()` must skip 500 and land on 499; if on 499, `next()` must skip 500 and land on 501.
- **Components:** `cur_layered.c`, tombstone skip during iteration
- **Notes:** Tests that iteration after `search_near` correctly handles a tombstone at the originally targeted key.

### `test_layered05.test_search_near_consecutive_tombstones`
- **What it tests:** All 1000 keys in stable. Ingest tombstones 400-600. Tests `search_near` at 400 (either 399 or 601), 500 (either 399 or 601), and 600 (must return 601). Then does a forward scan from `search_near(500)` and verifies no deleted key appears and all keys from 601-999 are present.
- **Components:** `cur_layered.c`, tombstone skip during iteration, boundary detection
- **Notes:** Verifies that a large contiguous range of tombstones is properly skipped in both `search_near` and subsequent iteration.

### `test_layered05.test_search_near_full_scan_interleaved`
- **What it tests:** Even keys 0-998 in stable, odd keys 1-999 in ingest. Positions at key 0 via `search_near`, then walks forward collecting all keys; asserts all 1000 keys appear in order. Then repeats backward from key 999.
- **Components:** `cur_layered.c`, full-scan merge of stable and ingest
- **Notes:** Validates the complete merge sort behavior of the layered cursor over 1000 interleaved keys.

### `test_layered05.test_search_near_ingest_overrides_stable`
- **What it tests:** All 1000 keys checkpointed, then all re-inserted locally with new values. `search_near(500)` returns exact match with the locally-written value, confirming ingest writes take precedence over stable.
- **Components:** `cur_layered.c`, stable/ingest precedence logic

### `test_layered05.test_search_near_beyond_max`
- **What it tests:** Keys 0-499 in stable, key 700 in ingest. `search_near(1100)` — beyond all keys — must return key 700 with exact=-1.
- **Components:** `cur_layered.c`

### `test_layered05.test_search_near_ingest_tombstone_no_stable_forward`
- **What it tests:** No checkpoint. Keys 100, 500, 700 inserted; 500 deleted. `search_near(500)`: exact deleted, either 100 or 700 is valid.
- **Components:** `cur_layered.c`, ingest btree only, tombstone

### `test_layered05.test_search_near_ingest_tombstone_no_stable_backward`
- **What it tests:** No checkpoint. Keys 100 and 500 inserted; 500 deleted. `search_near(500)` must return 100 with exact=-1.
- **Components:** `cur_layered.c`, ingest btree only

### `test_layered05.test_search_near_ingest_tombstone_no_stable_notfound`
- **What it tests:** No checkpoint. Only key 500 inserted then deleted. `search_near(500)` must return `WT_NOTFOUND`.
- **Components:** `cur_layered.c`, ingest btree only

### `test_layered05.test_search_near_tombstone_walk_then_prev`
- **What it tests:** All 1000 keys in stable. Ingest deletes 500-999. `search_near(700)` must land below 500. Subsequent `prev()` scan must produce all remaining keys in strictly descending order with no deleted keys appearing.
- **Components:** `cur_layered.c`, tombstone handling across half-deleted range, reverse iteration

### `test_layered05.test_search_near_tombstone_walk_then_next_with_bounds`
- **What it tests:** All 1000 keys in stable. Ingest deletes 300-600. A bounded cursor [200, 800] calls `search_near(450)` (key is deleted). Result is either 299 or 601. Subsequent `next()` scan must stay within bounds, produce keys in ascending order, and exclude all deleted keys.
- **Components:** `cur_layered.c`, cursor bounds, tombstone skip, bounded iteration
- **Notes:** Exercises the MongoDB-style pattern of set-bounds + search_near + iterate. Fails if bounds are not respected during skip-tombstone iteration, or if `search_near` doesn't account for bounds.
