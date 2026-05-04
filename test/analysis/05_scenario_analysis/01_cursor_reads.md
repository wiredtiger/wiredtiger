# Cursor Read Operations: Scenario Gap Analysis

## Coverage summary per API

| API | What's Tested | Notable Tests |
|-----|---------------|---------------|
| **cursor.search()** | Single exact key lookup in stable, ingest, and interleaved data; search across checkpoints; search within bounds; position for iteration. | test_layered81, test_layered82, test_layered83 |
| **cursor.search_near()** | Non-exact neighbors; exact matches; empty tables; stable-only, ingest-only, split data; tombstone handling; iteration after search_near; bounds + search_near; mid-scan checkpoint advance. | test_layered05 (extensive), test_layered82, test_layered85 |
| **cursor.next() / prev()** | Full forward/backward scan; positioned iteration; tombstone skipping; interleaved data ordering; bounded iteration; iteration across mid-scan checkpoint advances. | test_layered_cursor01, test_layered81, test_layered82, test_layered83, test_layered85 |
| **cursor.bound()** | Inclusive/exclusive bounds; bounds with stable-only, ingest-only, split data; bounds on non-existent keys; tombstones inside/outside bounds; single-point bounds; bound clearing; bounds persist across checkpoint; search_near within bounds. | test_layered82 (comprehensive), test_layered05, test_layered81, test_layered85 |
| **cursor.reset()** | Reset clears position; reset after checkpoint advance; reset clears bounds; unpositioned cursor sees new data after reset and checkpoint advance. | test_layered_cursor01, test_layered81, test_layered82, test_layered83 |

---

## Gap analysis per API

### cursor.search()

#### Covered scenarios
- Key in stable-only, ingest-only, both (ingest overrides)
- Key absent: WT_NOTFOUND
- Empty table: WT_NOTFOUND
- Search across checkpoint advance with positioned cursor
- Within bounds: respects lower/upper bounds
- Leader vs follower: both roles work
- After checkpoint advance: new keys found

#### Missing scenarios

**Gap 1 [HIGH]: Search for deleted key (tombstone in ingest)**
- Scenario: Key in stable, tombstone in ingest. search(key) should return WT_NOTFOUND.
- Why: If search incorrectly skips ingest tombstones, it returns stale data from stable.
- Suggested test: Extend test_layered82 or new test_layered_search_tombstone

**Gap 2 [MEDIUM]: Search with active read_timestamp (snapshot isolation)**
- Scenario: Transaction at read_timestamp=T1 searches for key inserted at T2 (T2 > T1). Should return WT_NOTFOUND.
- Why: Timestamp-based isolation is critical for correctness. Basic path covered by `test_layered73.py`; adversarial case (key at T2 > T1 must not be visible) is missing.
- Suggested test: Extend test_layered81

**Gap 3 [DEFERRED]: Search after follower role transition with prepared transactions**
- Scenario: Cursor repositions via search() after follower→leader transition with in-flight prepared transactions.
- Why: Production scenario; test_layered73 has prepared tests but not combined with search().
- Suggested test: Extend test_layered73
*(Prepared transactions not currently supported in disagg; see PT-2 in 08_unsupported_features.md.)*

**Gap 4 [MEDIUM]: Search for keys with binary or special-character format**
- Scenario: key_format=u (raw binary), null bytes, very long keys (near WT_KEY_MAX).
- Why: String comparison and encoding edge cases are unique to binary keys.
- Suggested test: New test_layered_search_key_formats

**Gap 5 [LOW]: Statistics accuracy after search**
- Scenario: Assert that cursor_search statistics increment correctly across both constituents.
- Suggested test: Add stat assertions to existing tests.

---

### cursor.search_near()

#### Covered scenarios
- Exact matches in stable, ingest, both
- Deleted exact match (tombstone): returns live neighbor
- Non-exact search returns correct neighbor
- Key before all data: returns smallest key (cmp=1)
- Key after all data: returns largest key (cmp=-1)
- Empty table: WT_NOTFOUND
- Interleaved data: correctly finds neighbors across layers
- Tombstone ranges: skipped correctly
- Followed by next/prev: produces sorted order
- With bounds: respects bounds
- Mid-scan checkpoint advance with read_timestamp: monotonic order preserved

#### Missing scenarios

**Gap 1 [HIGH]: search_near stability with equidistant neighbors**
- Scenario: Table has [100, 700]. search_near(400) can legitimately return either. Must return consistently on repeated calls from the same state.
- Why: Non-determinism breaks application invariants (e.g., range scans that must resume from a known position).
- Suggested test: New test specifically checking determinism

**Gap 2 [MEDIUM]: search_near with read_timestamp filtering**
- Scenario: Transaction at read_timestamp=T1. Stable has old data (T0). Ingest has new data (T2 > T1). search_near must exclude T2-era data and find neighbors only among T1-visible keys.
- Why: MongoDB range queries run in snapshot transactions; if search_near ignores the snapshot it returns wrong neighbors. Basic path covered by `test_layered73.py`; adversarial case (key at T2 > T1 must not be visible) is missing.
- Suggested test: Extend test_layered82 with timestamp scenarios

**Gap 3 [HIGH]: search_near on exact match that has a tombstone overlapping both layers**
- Scenario: Stable has [100, 500, 900]. Ingest has tombstone for key 500. search_near(500) must return either 100 or 900, not 500.
- Why: The most common use pattern in MongoDB; exact-match deletions are frequent.
- Suggested test: Extend test_layered05

**Gap 4 [MEDIUM]: search_near between very sparse layers**
- Scenario: Stable has keys at 1M, 2M, 3M. Ingest has single key at 1.5M. search_near(1.7M) chooses correct neighbor.
- Why: Most tests use dense, sequential data. Sparse data may reveal iterator handoff bugs.
- Suggested test: New test_layered_search_near_sparse

**Gap 5 [MEDIUM]: search_near followed by dynamic bound application**
- Scenario: search_near(key), then apply bounds and iterate. test_layered05 applies bounds before search_near, not after.
- Suggested test: Extend test_layered05

**Gap 6 [DEFERRED]: search_near with unresolved prepared transactions in ingest**
- Scenario: Ingest has a prepared (uncommitted) write or delete. search_near must handle visibility correctly.
- Suggested test: Extend test_layered73
*(Prepared transactions not currently supported in disagg; see PT-2 in 08_unsupported_features.md.)*

**Gap 7 [LOW]: search_near at btree page boundaries**
- Scenario: Keys positioned exactly at internal page splits in one or both btrees.
- Suggested test: New test with controlled page size

---

### cursor.next() / cursor.prev()

#### Covered scenarios
- Full forward/backward scan from all starting positions
- Tombstone skipping: deleted keys not returned
- Interleaved data ordering across stable + ingest
- Bounded iteration: respects lower/upper bounds
- Mid-scan checkpoint advance: continues sorted from position
- Empty table: WT_NOTFOUND immediately
- Positioned update mid-scan does not disrupt iteration order

#### Missing scenarios

**Gap 1 [MEDIUM]: next/prev with active read_timestamp (snapshot iteration)**
- Scenario: Transaction at read_timestamp=T1. next/prev shows only T1-visible keys in sorted order, skipping both out-of-snapshot and deleted keys across both btrees.
- Why: Snapshot iteration is the core MongoDB scan pattern. Basic path covered by `test_layered73.py`; adversarial case (key at T2 > T1 must not be visible) is missing.
- Suggested test: New test_layered_iteration_snapshot

**Gap 2 [HIGH]: next/prev with concurrent writes from another session**
- Scenario: Session A iterates forward. Session B inserts/deletes keys concurrently. Session A should see a consistent snapshot.
- Why: Concurrency correctness is the hardest class of bugs.
- Suggested test: New test_layered_iteration_concurrent

**Gap 3 [MEDIUM]: next/prev at the layer boundary (last stable key → first ingest key)**
- Scenario: Iterate to the exact transition point where all remaining keys come from ingest. Confirm no gap, no duplicate.
- Suggested test: Extend test_layered_cursor01

**Gap 4 [MEDIUM]: next() after failed search() (cursor not positioned)**
- Scenario: search(key) returns WT_NOTFOUND. Calling next() immediately should also return WT_NOTFOUND (cursor unpositioned).
- Suggested test: New test_layered_next_after_notfound

**Gap 5 [MEDIUM]: next/prev across a large contiguous tombstone range**
- Scenario: Keys 1-1000 all deleted in ingest. Keys 1001+ still live. next() from beginning should jump to 1001 efficiently.
- Why: Large delete batches (TTL) are common in production.
- Suggested test: Extend test_layered85

**Gap 6 [LOW]: prev() on completely unpositioned cursor**
- Scenario: prev() called on a brand-new cursor (never positioned). Should go to last key.
- Suggested test: Minor addition to any existing test

---

### cursor.bound()

#### Covered scenarios
- Inclusive/exclusive lower and upper bounds
- Bounds on stable-only, ingest-only, interleaved data
- Bounds on non-existent boundary keys
- Single-point bounds [key, key]
- Bounds on empty range (no data between lower and upper)
- Tombstones inside and outside bounds
- Tombstones at the exact bound keys
- Bound clearing with action=clear
- Bounds persist across checkpoint advance
- search_near within bounds
- search() respects bounds

#### Missing scenarios

**Gap 1 [HIGH]: Bounds + read_timestamp filtering combined**
- Scenario: Transaction at read_timestamp=T1. Bounds [200, 800]. next() should return only keys in [200,800] AND visible at T1.
- Why: This is precisely MongoDB's range-query-in-snapshot pattern. No test combines both filters.
- Suggested test: New test_layered_bounds_snapshot

**Gap 2 [HIGH]: Cursor bounds + role transition**
- Scenario: Cursor with bounds set when conn.reconfigure(role="leader") is called. Does __clayered_adjust_state correctly preserve or clear bounds when switching the stable constituent?
- Why: Bounds are stored on the cursor object; the constituent cursor is replaced during role change. The interaction is untested.
- Suggested test: New test_layered_bounds_role_transition

**Gap 3 [MEDIUM]: Dynamic bound rebinding during active iteration**
- Scenario: Iterate within [200, 800]. After advancing to key 400, rebind to [300, 700] without reset. Behavior undefined or documented?
- Suggested test: New test_layered_bounds_rebind

**Gap 4 [MEDIUM]: Invalid bounds (lower > upper)**
- Scenario: Set lower=800, upper=200. Expected: error or empty iteration?
- Suggested test: Add error-case test

**Gap 5 [MEDIUM]: Bounds on table after cursor-range truncate**
- Scenario: Bounded cursor [200, 800]. Table truncated. Cursor with bounds must return WT_NOTFOUND.
- Suggested test: Extend test_layered_fast_truncate series

**Gap 6 [LOW]: Overlapping bound sets (set bounds twice without clear)**
- Scenario: Set [200, 800], then set [300, 700] (no clear). Does second bind replace or combine?
- Suggested test: Small addition to bound tests

---

### cursor.reset()

#### Covered scenarios
- Reset clears cursor position
- next/prev after reset starts from beginning/end
- Reset clears bounds
- Unpositioned cursor after reset sees new data following checkpoint advance

#### Missing scenarios

**Gap 1 [MEDIUM]: Reset mid-iteration then restart from beginning**
- Scenario: Iterate 100 keys out of 1000, call reset(), iterate again. Verify no skipped or duplicated keys.
- Suggested test: New test_layered_reset_iteration

**Gap 2 [MEDIUM]: Reset search_near idempotence**
- Scenario: search_near(key) → get cmp. reset(). search_near(key) again. Same cmp, same key returned.
- Suggested test: New test_layered_reset_idempotence

**Gap 3 [MEDIUM]: Reset within snapshot transaction preserves isolation**
- Scenario: Begin txn with read_timestamp. search(key). reset(). search(key) again. Both calls see the same data.
- Suggested test: Extend any timestamp test

**Gap 4 [LOW]: Multiple consecutive resets**
- Scenario: reset(), reset(), reset(). Should be a no-op after the first.
- Suggested test: Minor addition

---

## Priority-ranked gap list

### HIGH
1. search() on tombstone key (stable value, ingest tombstone) → extend test_layered82
2. search_near stability with equidistant neighbors → new test
3. search_near on tombstone overlapping both layers → extend test_layered05
4. cursor.bound() + read_timestamp combined → new test
5. next/prev concurrent with writer session → new test
6. cursor.bound() + role transition → new test

### MEDIUM
7. search() + read_timestamp filtering → extend test_layered81 *(Basic path covered by `test_layered73.py`; adversarial case missing)*
8. search_near + read_timestamp filtering → extend test_layered82 *(Basic path covered by `test_layered73.py`; adversarial case missing)*
9. next/prev + read_timestamp (snapshot iteration) → new test_layered_iteration_snapshot *(Basic path covered by `test_layered73.py`; adversarial case missing)*
10. search_near sparse layers → new test
11. next/prev at layer boundary → extend test_layered_cursor01
12. next() after failed search → new test
13. next/prev across large tombstone range → extend test_layered85
14. search_near + dynamic bounds after positioning → extend test_layered05
15. Dynamic bound rebinding during iteration → new test
16. Bounds + slow truncate → test_layered_truncate_bounds
17. Reset mid-iteration → new test
18. Reset search_near idempotence → new test
19. Reset within snapshot transaction → extend timestamp tests

### LOW
20. Invalid bounds (lower > upper) → new test
21. search() binary/encoded keys → new test
22. Overlapping bound sets → minor addition
23. Unpositioned prev() → minor addition
24. Reset after tombstone skip → minor addition
25. search_near page boundaries → new test
26. Cursor statistics accuracy → add assertions to existing tests

### Deferred — Prepared Transactions (Target: Public Preview)
*search + prepared transactions (Gap 3), search_near + prepared transactions (Gap 6) — see `08_unsupported_features.md` (PT-1 through PT-5).*
