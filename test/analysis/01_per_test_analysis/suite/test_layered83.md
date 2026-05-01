# test_layered83 — Comprehensive cursor iteration and search/search_near on layered cursors

**File:** `test/suite/test_layered83.py`
**Storage mode:** Disagg/Layered
**Components under test:** Layered cursor `next`, `prev`, `search`, `search_near`, direction switching, tombstone skipping, writes during scan, ordering invariants

## Test Cases

### `test_layered83.test_next_full_scan`
- **What it tests:** Forward scan of 1000 interleaved keys (even in stable, odd in ingest) returns all keys in ascending order.
- **Components:** `src/cursor/cur_layered.c`, stable+ingest merge

### `test_layered83.test_prev_full_scan`
- **What it tests:** Backward scan of 1000 interleaved keys returns all keys in descending order.
- **Components:** Reverse iteration merge

### `test_layered83.test_next_duplicate_keys`
- **What it tests:** All 1000 keys in stable. Every 10th key overridden by local ingest write with value "ingest_XXXXXX". Full forward scan verifies: updated keys have ingest value, others have original value; no duplicate keys, total count = 1000.
- **Components:** Ingest-over-stable precedence during forward scan

### `test_layered83.test_next_skips_tombstones`
- **What it tests:** All keys in stable. Every 3rd key removed via ingest. Forward scan returns only non-removed keys.
- **Components:** Forward tombstone skipping

### `test_layered83.test_prev_skips_tombstones`
- **What it tests:** All keys in stable. Every 3rd key removed via ingest. Backward scan returns only non-removed keys.
- **Components:** Backward tombstone skipping

### `test_layered83.test_next_all_tombstoned`
- **What it tests:** All keys in stable, all removed via ingest. `cursor.next()` returns `WT_NOTFOUND`.
- **Components:** All-tombstoned empty forward scan

### `test_layered83.test_prev_all_tombstoned`
- **What it tests:** All keys in stable, all removed via ingest. `cursor.prev()` returns `WT_NOTFOUND`.
- **Components:** All-tombstoned empty backward scan

### `test_layered83.test_direction_switch_next_to_prev`
- **What it tests:** Interleaved data. Steps forward 501 times to land on key 500. Switches to backward 10 steps, ends at key 490.
- **Components:** `next`→`prev` direction switch correctness

### `test_layered83.test_direction_switch_prev_to_next`
- **What it tests:** Interleaved data. Steps backward 501 times to land on key 499. Switches to forward 10 steps, ends at key 509.
- **Components:** `prev`→`next` direction switch correctness

### `test_layered83.test_direction_zigzag`
- **What it tests:** Positions at key 500 via search, then performs 20 `next`/`prev` alternations. Each `next` must land at 501, each `prev` at 500.
- **Components:** Repeated direction switching on interleaved data (stable even, ingest odd)

### `test_layered83.test_next_after_search_stable_key`
- **What it tests:** Positions at stable key 400 via search, walks forward 10 steps. Verifies keys 401–410.
- **Components:** Iteration after search on a stable key

### `test_layered83.test_next_after_search_ingest_key`
- **What it tests:** Positions at ingest key 401 via search, walks forward 10 steps. Verifies keys 402–411.
- **Components:** Iteration after search on an ingest key

### `test_layered83.test_prev_after_search`
- **What it tests:** Positions at key 500, walks backward 10 steps. Verifies keys 499–490.
- **Components:** Backward iteration after search

### `test_layered83.test_prev_after_search_at_start`
- **What it tests:** Positions at key 0 (first key), calls `prev()`; expects `WT_NOTFOUND`.
- **Components:** `prev()` boundary at start of table

### `test_layered83.test_next_after_search_at_end`
- **What it tests:** Positions at key 999 (last key), calls `next()`; expects `WT_NOTFOUND`.
- **Components:** `next()` boundary at end of table

### `test_layered83.test_search_then_direction_switch`
- **What it tests:** Positions at key 500, walks forward 5 (lands at 505), switches to backward 10 (lands at 495). Verifies keys 501–505 then 504–495.
- **Components:** Search then multi-step direction switch

### `test_layered83.test_next_after_search_near_exact`
- **What it tests:** `search_near(600)` returns exact match (cmp=0). Walks forward 10 steps, verifies keys 601–610.
- **Components:** Forward iteration after exact `search_near`

### `test_layered83.test_prev_after_search_near_exact`
- **What it tests:** `search_near(600)` returns exact match. Walks backward 10 steps, verifies keys 599–590.
- **Components:** Backward iteration after exact `search_near`

### `test_layered83.test_next_after_search_near_larger`
- **What it tests:** `search_near(" before_all")` (key before all data) returns cmp>0 and lands on key 0. `next()` returns key 1.
- **Components:** `search_near` below table range, then forward iteration

### `test_layered83.test_prev_after_search_near_smaller`
- **What it tests:** `search_near("999999_after")` (key after all data) returns cmp<0 and lands on key 999. `prev()` returns key 998.
- **Components:** `search_near` above table range, then backward iteration

### `test_layered83.test_search_near_then_direction_switch`
- **What it tests:** `search_near(700)` exact match. Forward 5 steps (701–705), then backward 1 step (704).
- **Components:** Direction switch after `search_near`

### `test_layered83.test_next_after_search_with_tombstones`
- **What it tests:** Keys 501–509 tombstoned. Positions at key 500, `next()` skips the gap and lands at key 510.
- **Components:** Tombstone skipping in `next()` after search

### `test_layered83.test_prev_after_search_with_tombstones`
- **What it tests:** Keys 491–499 tombstoned. Positions at key 500, `prev()` skips the gap and lands at key 490.
- **Components:** Tombstone skipping in `prev()` after search

### `test_layered83.test_iterate_after_search_near_tombstone`
- **What it tests:** Key 500 tombstoned. `search_near(500)` returns cmp=1, key=501. `prev()` returns key 499 (skips the tombstone at 500).
- **Components:** `search_near` on tombstoned key, then backward iteration

### `test_layered83.test_repeated_search_iterate`
- **What it tests:** Three search+iterate cycles on the same cursor: search(100)+next×5=101–105, search(800)+next×5=801–805, search(300)+prev×5=299–295.
- **Components:** Cursor reuse across multiple search+iterate cycles

### `test_layered83.test_mixed_search_near_and_search`
- **What it tests:** `search_near(200)` exact+`next()`=201, then `search(600)`+`prev()`=599, on the same cursor.
- **Components:** Mixing `search_near` and `search` on the same cursor

### `test_layered83.test_reset_between_search_iterate`
- **What it tests:** search(500)+next()=501, reset, next()=0 (first key).
- **Components:** `reset()` clears cursor position for re-scan from start

### `test_layered83.test_next_empty` / `test_prev_empty`
- **What it tests:** `next()` and `prev()` on an empty table both return `WT_NOTFOUND`.
- **Components:** Empty table edge cases

### `test_layered83.test_next_after_end_then_rescan`
- **What it tests:** Exhausts forward scan, resets, scans again — same 1000 keys returned.
- **Components:** Cursor reset after exhaustion for re-scan

### `test_layered83.test_next_ingest_only`
- **What it tests:** All 1000 keys written locally (no checkpoint). Forward scan returns all keys.
- **Components:** Ingest-only forward scan (no stable btree)

### `test_layered83.test_next_stable_only`
- **What it tests:** All 1000 keys checkpointed. Forward scan returns all keys.
- **Components:** Stable-only forward scan (no ingest)

### `test_layered83.test_next_after_search_near_xor_alternate_behind`
- **What it tests:** Table: stable key=200, ingest keys=300,600. `search_near(500)` returns 300 or 600 (adjacent neighbor). Forward scan of remaining keys after `search_near` must be in ascending order.
- **Components:** Ordering invariant when `search_near` picks an alternate sub-cursor result

### `test_layered83.test_prev_after_search_near_xor_alternate_ahead`
- **What it tests:** Table: stable key=800, ingest keys=400,700. `search_near(500)` returns 400 or 700. Backward scan of remaining keys must be in descending order.
- **Components:** Descending order invariant after `search_near` with alternate sub-cursor

### `test_layered83.test_next_after_search_near_both_smaller`
- **What it tests:** Stable=300, ingest=400. `search_near(500)` returns key=400 (cmp=-1). Forward scan remains ascending.
- **Components:** `search_near` below the searched key, ascending continuation

### `test_layered83.test_prev_after_search_near_both_larger`
- **What it tests:** Stable=700, ingest=600. `search_near(500)` returns key=600 (cmp=1). Backward scan remains descending.
- **Components:** `search_near` above the searched key, descending continuation

### `test_layered83.test_next_after_search_near_xor_many_keys`
- **What it tests:** Even keys 0–400 stable, odd keys 501–999 ingest. Key 450 inserted after a read timestamp snapshot. `search_near(450)` at the snapshot timestamp returns key=400 (cmp=-1). Forward scan is ascending and does not include the later-inserted 450.
- **Components:** Timestamp snapshot isolation with `search_near`, ascending order

### `test_layered83.test_prev_after_search_near_xor_many_keys`
- **What it tests:** Even keys 600–998 stable, odd keys 1–499 ingest. Odd keys removed after a snapshot. `search_near(550)` at the snapshot returns key=600 (cmp=1). Backward scan is descending and odd ingest keys are visible under the snapshot.
- **Components:** Timestamp snapshot isolation with `search_near`, descending order

### `test_layered83.test_next_after_search_near_xor_with_tombstones`
- **What it tests:** All 1000 keys stable, keys 400–600 tombstoned. Key 450 re-inserted after snapshot. `search_near(500)` at snapshot returns key=601 (cmp=1). Forward scan is ascending; tombstoned keys 400–600 do not appear.
- **Components:** `search_near` past a tombstoned range, ascending order, timestamp isolation

### `test_layered83.test_next_after_search_near_interleaved_full_coverage`
- **What it tests:** All 1000 interleaved keys. For each of search_pos in {0, 100, 250, 500, 750, 999}: key removed after a snapshot, then `search_near(search_pos)` at the snapshot finds exact match, and forward scan is in strictly ascending order.
- **Components:** Full coverage of ascending-order invariant at 6 anchor positions

### `test_layered83.test_prev_after_search_near_interleaved_full_coverage`
- **What it tests:** Same as above but uses `prev()` and verifies strictly descending order.
- **Components:** Full coverage of descending-order invariant at 6 anchor positions

### `test_layered83.test_positioned_update_mid_scan`
- **What it tests:** Interleaved data. Searches key=500, advances 5 steps. Performs a positioned update at the current key (writes "updated"). Continues scanning. Verifies all keys returned are in strictly ascending order and the scan does not go backward from the update position.
- **Components:** Positioned update mid-scan, iteration order preservation
