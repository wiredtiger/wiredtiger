# Gap Analysis: Layered Table Cursor Operations (cur_layered.c)

*Coverage analyzed against: test_layered05, 22, 41, 55, 57, 58, 96, test_layered_cursor01, test_layered_fast_truncate01–03, test_layered_modify01*

*Source analyzed: `src/cursor/cur_layered.c` (~2852 lines), full read*

*Date: 2026-05-01*

---

## Current Coverage Summary

| Cursor operation | Leader covered | Follower covered | Notes |
|---|---|---|---|
| `next` | Yes (cursor01) | Yes (cursor01, layered22) | Iteration across both btrees tested |
| `prev` | Yes (cursor01) | Yes (cursor01, layered22) | Reverse full scan tested |
| `search` | Yes (cursor01) | Yes (cursor01, layered22) | Ingest-only and split-data cases |
| `search_near` | Yes (layered05) | Yes (layered05, cursor01) | 29 dedicated test cases |
| `insert` | Yes (cursor01) | Yes (cursor01) | overwrite=false in layered41 |
| `update` | Yes (cursor01) | Yes (cursor01) | Partial (20–70%) update density |
| `remove` | Yes (cursor01) | Yes (cursor01) | Partial (20–70%) removal density |
| `modify` (follower) | No | Yes (layered22, modify01) | stable-sourced base value tested |
| `modify` (leader) | No direct test | No | Not directly exercised in isolation |
| `reserve` | Yes (layered92/93) | Yes (layered92/93) | State matrix covered; commit path (reserve+write+commit) untested (CW-H7) |
| `largest_key` | No | Yes (layered22) | ingest-only; stable+ingest merge not tested |
| `bound` | Yes (layered82) | Yes (layered82) | Comprehensive: inclusive/exclusive, all key locations, tombstones, checkpoint, search+bound, reset+bound |
| `reset` | Yes (layered82) | Yes (layered82) | Reset clears bounds verified; reset+iteration restart verified |
| `close` / `reopen` | Implicit | Implicit | Cache/reopen paths not directly tested |
| `next_random` | No | Yes (layered22) | Only ingest-only tested |
| `compare` | No | No | No dedicated test |
| Checkpoint advance | Yes (fast_truncate) | Yes (fast_truncate) | Stable cursor advancement tested |
| Step-up / step-down | No direct | No direct | FIXME-WT-14545 acknowledged in code |

---

## Duplicate / Overlapping Cases

### `test_layered05.test_search_near_ingest_only` vs `test_layered22.test_secondary_search_without_stable`

Both tests exercise `search_near` on a table with only ingest data and no checkpoint. `test_layered22` is a simple not-found / found check. `test_layered05.test_search_near_ingest_only` is more thorough (boundary cases, between-two-keys). The tests do not conflict but the simpler case in `test_layered22` adds no unique value once `test_layered05` exists. Low duplication risk; both can coexist.

### `test_layered05.test_search_near_ingest_tombstone_no_stable_forward` vs `test_layered05.test_search_near_ingest_tombstone_no_stable_backward` vs `test_layered05.test_search_near_ingest_tombstone_no_stable_notfound`

These three tests form a set for ingest-only tombstone handling. The first two share setup (insert then delete) and differ only in whether a living neighbor exists above or below. The third is the all-deleted edge case. These are logically distinct and not genuine duplicates.

### `test_layered_cursor01` positioned-iteration variants (search, search_near, next, prev as `pos_func`)

`test_layered_cursor01` is parametrized by four positioning methods (`search`, `search_near`, `next`, `prev`). When the table has only insert operations (no remove, no update), the behavior of positioned iteration after any of these four methods is identical—all keys are live, all values are the same, and the merge cursor is never exercised for tombstone skipping. The four sub-scenarios therefore produce identical coverage for pure-insert workloads. The distinction only becomes meaningful in the `with_removes` and `with_updates` variants. This is a documentation-level concern, not a code defect.

### `test_layered_cursor01.test_populated_tables_with_updates_20_percent` (two definitions)

The analysis markdown notes two definitions of `test_populated_tables_with_updates_20_percent` — one with `updates_offset=0` and one with a non-zero offset. If both are present in the actual `.py` file under different names (the analysis notes "(second definition — offset variant)"), they are distinct. If one truly shadows the other in the class body, Python will silently keep only the last definition. This should be verified in the source.

---

## Missing Coverage

---

### [HIGH] Gap 1: `cursor->reserve()` commit path is untested on layered tables

**What is covered:**
`test_layered92.py` and `test_layered93.py` cover `cursor.reserve()` across all key-location
states × {leader, follower}: stable-only key, ingest-only key, key in both btrees, and missing
key. Both tests verify that reserve correctly positions the cursor and that `cursor.get_value()`
returns the pre-reserve value. These tests exercise `__clayered_reserve`, `__clayered_lookup`,
`__clayered_reserve_constituent`, and `__clayered_put(WT_CLAYERED_PUT_RESERVE)`.

**What is still not tested:**
Every existing reserve test rolls back the transaction after the reserve. No test completes the
round-trip: `reserve → write the new value → commit`. The full write path after a reserve —
where `cursor.update()` or `cursor[key] = newval` is called while the reserve is held and then
committed — is entirely absent. This is the production use-case: reserve is used for optimistic
locking and the lock is only useful if a write can follow.

**Risk:**
A bug in the write-after-reserve path (e.g., the update uses the wrong btree target, or the
write-lock from the reserve is not correctly respected by a concurrent write) would cause silent
data corruption or false `WT_ROLLBACK` under concurrency. The FIXME comment at line 2271
("any previous value in the cursor might race with WT_CURSOR.reserve") signals this path has
known subtle semantics.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_reserve()` — lines 2238–2279
- `src/cursor/cur_layered.c:__clayered_put()` — lines 1892–1937, the update-after-reserve write
- Triggered when: `cursor.reserve()` followed by `cursor.update()` or value-set and commit

**Proposed test design:**
- Operations: `reserve(K)` → `cursor[K] = newval` → `commit_transaction()`
- Assertions: After commit, `cursor.search(K)` returns newval. A concurrent update started after
  the reserve (before commit) returns `WT_ROLLBACK`.
- Suggested test: `test_layered_reserve_update01.py` (CW-H7 in the scenario analysis)

---

### [CRITICAL] Gap 2: `search_near` tombstone handling when `closest` is ingest and exact match is deleted, and `__clayered_iterate_int(NEXT)` returns `WT_NOTFOUND`, leaving `deleted=true` for the fallback `PREV` path — and the fallback `PREV` also returns `WT_NOTFOUND`

**What is not tested:**
`__clayered_search_near_int` (lines 1779–1795) handles the case where the ingest cursor lands on a deleted exact match by calling `__clayered_iterate_int(NEXT)`. If `NEXT` finds nothing (all keys above are also deleted or exhausted), it sets `deleted=true` and calls `__clayered_iterate_int(PREV)`. The test `test_search_near_ingest_exact_deleted_all_tombstoned` covers the case where all three stable keys (300, 500, 700) are deleted by ingest, returning `WT_NOTFOUND`. However, it does not exercise the precise code path where `NEXT` exhausts itself (returning `WT_NOTFOUND`) and then `PREV` also returns `WT_NOTFOUND` because the cursor state after a failed `NEXT` is ambiguous. Specifically: after `__clayered_iterate_int(NEXT)` returns `WT_NOTFOUND`, `__clayered_reset_cursors` is called inside that function (lines 1097–1101), setting `current_cursor = NULL`. Then `deleted` remains `true` and the code calls `__clayered_iterate_int(PREV)`. At this point both constituent cursors are unpositioned, and the `PREV` walk starts from the end of the table. The correctness of this two-step recovery depends on the two walks together not missing keys. The existing test only verifies the final return code is `WT_NOTFOUND`, not the intermediate cursor state.

**Risk:**
If any key survives in either btree and is exactly equal to the search key, but the ingest tombstone covers it, the double-iteration recovery could land on the wrong neighbor (one further away than the actual nearest). This is a correctness bug in the `search_near` return contract.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_search_near_int()` — lines 1779–1795, the two-phase iterate-next-then-iterate-prev recovery
- `src/cursor/cur_layered.c:__clayered_iterate_int()` — lines 1081–1102, calls `__clayered_reset_cursors` on any non-PREPARE_CONFLICT error (including `WT_NOTFOUND` from line 1098)
- Triggered when: ingest has exact-match tombstone, all keys above are deleted/absent, but keys below exist in stable
- Existing tests miss it because: `test_search_near_ingest_exact_deleted_walk_backward` only has one survivor (key 200) which is found by the `PREV` walk, but the `NEXT` path is taken first and does successfully return `WT_NOTFOUND` before switching to `PREV` — the cursor reset logic is implicitly exercised but the neighbor correctness is not verified precisely

**Proposed test design:**
- Setup: Stable has keys {100, 500}; ingest deletes key 500 and all keys 501–1000 are absent; `search_near(500)` — ingest lands on 500 (deleted, exact match), `NEXT` exhausts (nothing > 500 in either tree), then `PREV` should find key 100
- Assertions: Return is 0; result key is 100; `exact=-1`; subsequent `prev()` returns `WT_NOTFOUND`; subsequent `next()` returns `WT_NOTFOUND` (only one live key)

---

### [CRITICAL] Gap 3: `cursor.modify()` on a key that exists only in ingest as a tombstone (follower path through `__clayered_modify_follower` where `ret == WT_NOTFOUND` and `__wt_clayered_deleted` is true)

**What is not tested:**
`__clayered_modify_follower` (lines 2562–2622) handles three cases for the ingest-positioned branch: (a) `ret == WT_NOTFOUND` (key was not found at all), (b) `__wt_clayered_deleted(&ingest->value)` (key is a tombstone in ingest), (c) `__clayered_is_deleted_encoded(&ingest->value)` (value starts with tombstone escape sequence). In all three cases it applies the modify delta to a zero-length base and writes the result as a full update. `test_layered22` and `test_layered_modify01` both test modifying existing, live keys. No test applies `cursor.modify()` to a key that has been deleted in ingest (tombstone present), which exercises path (b).

**Risk:**
If `modify()` on a tombstoned key incorrectly applies the delta to the tombstone bytes rather than treating the base as empty, the written value will be garbage. This is a data corruption bug affecting any workload where a key is deleted and then a `modify()` is applied (e.g., a prepare-and-abandon pattern followed by a new modify in the same ingest epoch).

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_modify_follower()` — lines 2596–2606, specifically the branch `if (ret == WT_NOTFOUND || __wt_clayered_deleted(&ingest->value) || __clayered_is_deleted_encoded(&ingest->value))`
- Triggered when: key was previously removed (tombstone in ingest), then `cursor.modify()` is called with the cursor positioned on that key or with key set externally
- Existing tests miss it because: `test_layered22` only modifies live keys; `test_layered_modify01` only uses keys from a clean stable checkpoint with no deletions

**Proposed test design:**
- Setup (follower, no stable): Insert key=50 value="AAAAAAAAAA"; remove key=50 (tombstone written); apply `cursor.modify([offset=2, size=3, data="BBB"])` to key=50
- Assertions: `cursor.get_value()` returns "AABBBAAAAA" (modify applied to empty/original? — clarify semantics); or verify that the implementation treats tombstone-as-empty correctly
- Also test: stable has key=50 value="base"; ingest deletes key=50; `cursor.modify()` on key=50 — should treat base as the stable value after the tombstone is stripped, or fail with `WT_NOTFOUND`

---

### [HIGH] Gap 4: `cursor.update()` with `overwrite=false` (no-overwrite update on non-existent key) on a layered follower

**What is not tested:**
`__clayered_update` (lines 2123–2177) checks `!F_ISSET(cursor, WT_CURSTD_OVERWRITE)` and if true calls `__clayered_lookup` to confirm the key exists before writing. `test_layered41` only tests `insert` with `overwrite=false`. No test exercises `update` with `overwrite=false` on a key that: (a) exists only in stable (not ingest), (b) does not exist at all, or (c) has been deleted in ingest (tombstone). On a follower, `__clayered_lookup` checks ingest first (finding a tombstone), which returns `WT_NOTFOUND` — and then `update` must propagate that failure without writing a new value. But if the tombstone is present in ingest, `__clayered_lookup` returns `WT_NOTFOUND` and the caller's `WT_ERR` path is taken correctly. If the key is only in stable, `__clayered_lookup` falls through to the stable search and finds it. The code appears correct but is never exercised.

**Risk:**
An untested no-overwrite update could silently overwrite a key that was deleted by a concurrent ingest tombstone, producing a phantom re-insertion. Or it could fail to update a key that legitimately exists in stable, returning a false `WT_NOTFOUND`.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_update()` — lines 2148–2155, the `!WT_CURSTD_OVERWRITE` branch calls `__clayered_lookup`
- `src/cursor/cur_layered.c:__clayered_lookup()` — lines 1508–1555, checks ingest tombstone (returns `WT_NOTFOUND`) before falling to stable
- Triggered when: `cursor opened with overwrite=false`, then `cursor.update()` is called
- Existing tests miss it because: `test_layered41` only tests `insert`; `test_layered_cursor01` uses the default `overwrite=true` mode

**Proposed test design:**
- Setup: Leader writes key=10 and checkpoints; follower picks up checkpoint
- Operations: Open follower cursor with `overwrite=false`; `cursor.update(key=10, value="new")` — must succeed (key in stable)
- Operations: `cursor.update(key=999, value="new")` — must return `WT_NOTFOUND` (key absent)
- Operations: Ingest-delete key=10; then `cursor.update(key=10, value="new")` — must return `WT_NOTFOUND` (tombstone shadows stable)
- Assertions: Correct error codes; no phantom writes; subsequent `cursor.search(key=10)` returns `WT_NOTFOUND` after ingest delete

---

### [HIGH] Gap 5: `largest_key()` when the largest key in ingest is a tombstone (deleted)

**What is not tested:**
`__clayered_largest_key` (lines 2286–2357) calls `ingest_cursor->largest_key()` and `stable_cursor->largest_key()` and compares. The constituent `largest_key()` on a btree cursor does not check for tombstones — it returns the physically largest key regardless of value. If the largest key in the ingest btree is a tombstone (the key was deleted), `__clayered_largest_key` will still return that key as the largest, even though it is not visible.

**Risk:**
`cursor.largest_key()` is documented to return the largest key visible to the caller. Returning a deleted key as "largest" violates this contract and can cause MongoDB's range key tracking to include phantom keys, leading to incorrect query plan bounds.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_largest_key()` — lines 2311–2344, no tombstone check on the ingest constituent's result
- The constituent `ingest_cursor->largest_key()` (file cursor) returns the physically last key without MVCC filtering for tombstones
- Triggered when: the largest key ever written to ingest has been subsequently deleted via `cursor.remove()`
- Existing tests miss it because: `test_layered22.test_largest_key_without_stable` only has live keys; no test deletes the largest key then calls `largest_key()`

**Proposed test design:**
- Setup (follower ingest-only): Insert keys "A", "B", "C"; remove key "C"; call `cursor.largest_key()`
- Assertions: Result must be "B" (not "C"); no `WT_NOTFOUND` returned
- Also test: stable has key "Z" (largest overall); ingest deletes "Z"; `largest_key()` must find next largest key. This requires a stable-+ingest combined scenario.

---

### [HIGH] Gap 6: `cursor.bound()` set after constituent cursors are already open (late-bound propagation)

**What is not tested:**
`__clayered_bound` (lines 1325–1371) copies bounds to constituent cursors. `__clayered_copy_bounds` is also called from `__clayered_open_cursors` (line 668) and from `__clayered_advance_stable` (line 453). The code comment in `__clayered_bound` notes: "the constituent cursors may not be open yet, and that would be fine." What is not tested is the case where bounds are set on a cursor that is already open and positioned (or at least opened), and then a checkpoint advance replaces the stable cursor — does the new stable cursor inherit the bounds correctly? `__clayered_advance_stable` calls `__clayered_copy_bounds` (line 453) which should handle this, but it is not verified.

**Risk:**
If a checkpoint advances while a bounded cursor is in mid-iteration, the new stable cursor may be opened without bounds, allowing it to return keys outside the declared range. This breaks the MongoDB range scan pattern.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_advance_stable()` — line 453, calls `__clayered_copy_bounds`
- `src/cursor/cur_layered.c:__clayered_copy_constituent_bound()` — lines 1272–1306
- Triggered when: bounds set on a layered cursor, then a new follower checkpoint is picked up mid-iteration
- Existing tests miss it because: `test_layered05.test_search_near_tombstone_walk_then_next_with_bounds` sets bounds before the first operation but never crosses a checkpoint boundary mid-walk

**Proposed test design:**
- Setup: Leader writes keys 1–1000, checkpoints; follower opens cursor with bounds [200, 800]; starts iterating (calls `next()` a few times inside bounds)
- Mid-walk: Leader writes more data, takes second checkpoint; follower picks it up (checkpoint_meta_lsn advances)
- Operations: Continue calling `next()` on the same cursor
- Assertions: No key outside [200, 800] is ever returned; iteration resumes correctly after the stable cursor is swapped out

---

### [HIGH] Gap 7: `next_random` when both ingest and stable have data (not ingest-only)

**What is not tested:**
`__clayered_next_random` (lines 2456–2518) prefers `stable_cursor` when both are available. It calls `__wti_curfile_next_random(stable)` and only falls back to ingest if stable returns `WT_NOTFOUND`. The current `test_layered22.test_getrandom_without_stable` only exercises the ingest-only fallback path. The combined stable+ingest path (where the randomly selected stable key may be a tombstoned key in ingest, resolved by `__clayered_search_near_int`) is never tested.

**Risk:**
If a randomly selected stable key has a tombstone in ingest, `__clayered_search_near_int` is called to find the nearest live key. If that neighbor happens to be outside a caller-expected range (or is itself deleted), the random cursor could return incorrect results silently. The FIXME at line 2474 ("consider the size of ingest table in the future") also suggests the current bias toward stable data is known-incomplete.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_next_random()` — lines 2473–2500, stable preferred, ingest fallback, then `__clayered_search_near_int` for tombstone resolution
- Triggered when: `next_random=true` cursor on layered table where stable has data and some stable keys are tombstoned in ingest
- Existing tests miss it because: `test_layered22` has no stable data; no other test exercises `next_random` on a table with both stable and ingest content

**Proposed test design:**
- Setup: Leader writes 10,000 keys and checkpoints; follower picks up checkpoint; follower ingest deletes 50% of those keys
- Operations: Open `next_random=true` cursor; call `next()` 100 times
- Assertions: Every returned key is a live key (not tombstoned in ingest); `cursor.get_value()` does not return a tombstone-encoded value; no assertion failure in `__clayered_search_near_int`

---

### [HIGH] Gap 8: `cursor.remove()` on a key that is currently being iterated (positioned remove during forward/backward scan)

**What is not tested:**
`__clayered_remove` (lines 2184–2231) preserves iteration flags when `positioned=true` (lines 2199–2202) and does not reset the cursor position. For a follower, `__clayered_remove_follower` (lines 1944–1995) checks `if (clayered->current_cursor == c)` to detect whether the cursor is already on the ingest table, and skips re-writing a tombstone if one is already there. The case where `positioned=true` and `current_cursor == stable_cursor` (key found in stable, not yet in ingest) requires the remove to look up the key's existence in stable (via `__clayered_lookup` at line 1967), write a tombstone to ingest, and then re-position the layered cursor on ingest for continued iteration. No test does a positioned remove during an active iteration and then continues iterating.

**Risk:**
If the cursor position is not correctly maintained after a positioned remove from the stable-sourced position, the next `cursor.next()` or `cursor.prev()` call could re-visit the same key (not fully skipping it), skip an adjacent key, or crash with an assertion due to inconsistent `iter_flag` state.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_remove()` — lines 2184–2231, `positioned` flag handling
- `src/cursor/cur_layered.c:__clayered_remove_follower()` — lines 1970–1989, when `current_cursor != ingest`, sets key on ingest and writes tombstone; `current_cursor` is updated to ingest after the remove
- `src/cursor/cur_layered.c:__clayered_put()` — lines 1910–1911, preserves iteration flags when iterating
- Triggered when: cursor positioned on a stable key during `next/prev` scan, then `cursor.remove()` is called
- Existing tests miss it because: `test_layered_cursor01` removes keys before scanning, not during; no test combines positioned remove with active iteration

**Proposed test design:**
- Setup: Stable has keys {1, 2, 3, 4, 5}; no ingest data; follower calls `cursor.next()` until positioned on key=3
- Operations: Call `cursor.remove()` (positioned remove); then call `cursor.next()` twice
- Assertions: After remove, `cursor.next()` returns key=4 then key=5; key=3 does not appear again; subsequent forward scan from the beginning shows keys {1, 2, 4, 5}

---

### [MEDIUM/DEFERRED] Gap 9: `__clayered_adjust_state` race: step-down during an active explicit transaction

**Status: DEFERRED — requires elegant step-down (SD-4 in `08_unsupported_features.md`)**

Elegant step-down (`conn.reconfigure(role="follower")`) is not yet supported in disagg (Public Preview
target). Testing this gap requires the ability to reconfigure a running leader to follower while an
in-flight transaction is open, which is precisely the SD-4 scenario. Implement when elegant step-down
is available.

**What is not tested:**
`__clayered_adjust_state` (lines 489–583) checks for leadership changes. If the node steps down from leader to follower while `session->txn->mod_count != 0` (modifications in flight), it sets `WT_ROLLBACK` and returns that error. This path is important for correctness but has no test. The FIXME at line 515 (`FIXME-WT-14545`) explicitly acknowledges the risk of undetectable step-down-then-step-up sequences.

**Risk:**
If the step-down check is wrong (for example, `mod_count` is 0 but there are prepared updates), the transaction may proceed as a leader transaction while the node is already a follower, writing to the stable btree as if it were the primary. This could corrupt the checkpoint.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_adjust_state()` — lines 523–533, the step-down branch with `WT_ROLLBACK` when `mod_count != 0`
- Triggered when: leader has an open transaction with writes, then leadership changes before commit
- Existing tests miss it because: all leadership-change tests run to completion before any state check; no test deliberately changes role mid-transaction

**Proposed test design:**
- Setup: Leader opens a transaction, inserts key=1 (mod_count becomes non-zero); simulate step-down by reconfiguring role (`conn.reconfigure("role=follower")`)
- Operations: Attempt any cursor operation on the layered table (triggers `__clayered_enter` → `__clayered_adjust_state`)
- Assertions: The operation returns `WT_ROLLBACK`; no data from the aborted transaction appears in the stable btree

---

### [SUPERSEDED] Gap 10: Bounds on `search` (not `search_near`) on a layered cursor

**Status: SUPERSEDED by `test_layered82.py`**

`test_layered82.py` provides comprehensive `cursor.bound()` coverage including `search()` with
bounds set (both in-bounds and out-of-bounds keys, across stable-only / ingest-only / interleaved
data, with inclusive and exclusive bounds). The original claim that all bounds tests go through
`search_near` only was based on `test_layered05`; `test_layered82` was added later and fills
this gap.

The remaining bound-related gaps (tracked in the scenario analysis) are:
- **CR-H1**: `cursor.bound()` + `read_timestamp` combined (snapshot range query pattern) — no test combines both filters
- **CR-H6**: `cursor.bound()` + role transition — bounds interaction with `__clayered_adjust_state` during step_up is untested

These are tracked in `05_scenario_analysis/01_cursor_reads.md` as HIGH gaps.

---

### [SUPERSEDED] Gap 11: `cursor.reset()` clears bounds — subsequent iteration without re-setting bounds

**Status: SUPERSEDED by `test_layered82.py`**

`test_layered82.py` explicitly tests that `cursor.reset()` with `action=clear` removes bounds and
that subsequent iteration is unbounded — keys outside the previously declared range are returned
after reset. The original claim that no test verifies the reset-clears-bounds contract was based
on `test_layered05`; `test_layered82` was added later and fills this gap.

The remaining reset-related gaps (tracked in the scenario analysis as MEDIUM) are:
- Reset mid-iteration then restart from beginning (no duplicate/skipped keys)
- Reset `search_near` idempotence (`search_near(key)`, `reset()`, `search_near(key)` — same result)
- Reset within a snapshot transaction preserves isolation

These are tracked in `05_scenario_analysis/01_cursor_reads.md`.

---

### [MEDIUM] Gap 12: Concurrent cursors on the same layered table, one positioned mid-scan while another modifies

**What is not tested:**
No test opens two cursors on the same layered table simultaneously (within the same connection/session or across two sessions on the same follower), with one cursor mid-scan and the other writing. The layered cursor does not take any exclusive lock on the constituent btrees during iteration; it relies on MVCC. The question is whether a write from Cursor B (on ingest) is correctly visible or hidden from Cursor A's ongoing scan according to Cursor A's transaction snapshot.

**Risk:**
If `__clayered_iterate_constituents` repositions the alternate cursor (via `__clayered_position_alternate`) in a way that picks up writes newer than Cursor A's snapshot, the scan produces a non-repeatable read at snapshot isolation. This is a subtle correctness bug.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_position_alternate()` — lines 875–904, calls `alternate->search_near()` and then steps forward/backward; snapshot isolation of the alternate cursor is controlled by the session snapshot, not re-snapshotted at each alternate positioning
- Triggered when: Two cursors share a session (or sessions share a connection) and one writes while the other scans
- Existing tests miss it because: All tests use a single cursor at a time per session; `test_layered_fast_truncate01` has two sessions but they don't share a mid-scan cursor

**Proposed test design:**
- Setup: Follower has stable data; Session A opens cursor A, calls `next()` to position on key=50; Session B (same follower connection) inserts key=51 into ingest and commits
- Operations: Session A calls `next()` again
- Assertions: At snapshot isolation, Session A's cursor must not see key=51 if it pre-dates Session A's snapshot; at read-committed, Session A may see key=51 depending on implementation; verify exact behavior matches documented isolation level

---

### [MEDIUM] Gap 13: `__clayered_iterate_int` skipping tombstones on the stable side

**What is not tested:**
`__clayered_iterate_int` (lines 1081–1102) checks for tombstones only when `current_cursor == ingest_cursor`. The stable cursor can never return a tombstone (it uses actual btree deletes, not encoded tombstones). However, stable data can be shadowed by an ingest tombstone. The tombstone check in `__clayered_iterate_int` is:

```c
if (clayered->current_cursor == clayered->ingest_cursor)
    deleted = __wt_clayered_deleted(&clayered->current_cursor->value);
else
    deleted = false;
```

This means: when `__clayered_get_current` selects the stable cursor as current (because ingest cursor is at the same key but ingest value is a tombstone), the stable key would be returned even though it has a covering tombstone. But wait — `__clayered_get_current` preferentially selects ingest over stable when both are at the same key (line 720: `cmp == 0 → current = ingest_cursor`). So the tombstone is always on the ingest cursor when keys match, and the tombstone check is exercised. The edge case not tested: what if `__clayered_get_current` picks stable as current because ingest is _not_ positioned at this key (it was advanced past it), but the stable key should be hidden by a tombstone that was committed to ingest _during_ iteration?

**Risk:**
Medium — the MVCC snapshot should prevent a newly committed tombstone from being visible in an in-progress scan. But at read-committed isolation there is no such protection.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_get_current()` — lines 678–731, stable wins only when ingest has no key at the same position
- `src/cursor/cur_layered.c:__clayered_iterate_int()` — lines 1081–1102, tombstone checked only on ingest
- Triggered when: read-committed scan, ingest tombstone committed mid-iteration, stable cursor is currently the "winner"

**Proposed test design:**
- Setup: Read-committed connection; stable has key=100; ingest adds tombstone for key=100 after scan starts but before cursor reaches key=100
- Operations: Forward scan from beginning
- Assertions: key=100 does not appear in results (tombstone committed before the scan reaches it)

---

### [LOW] Gap 14: `cursor.compare()` between two layered cursors with custom collators

**What is not tested:**
`__clayered_compare` (lines 738–762) uses the collator from the layered table handle. No test exercises `cursor.compare()` on a layered table at all. The function does verify that both cursors reference the same URI (otherwise `EINVAL`). There is no test for the cross-cursor comparison or the EINVAL error path.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_compare()` — lines 738–762
- Triggered when: two open layered cursors are passed to `cursor.compare()`
- Existing tests miss it because: cursor comparison is not needed in the Oplog-based test harness

---

### [LOW] Gap 15: `cursor.modify()` on the leader path (`__clayered_modify_leader`) when key has a delete-encoded value in stable

**What is not tested:**
`__clayered_modify_leader` (lines 2525–2555) handles the case where the stable btree has a delete-encoded value (lines 2541–2547: `__clayered_is_deleted_encoded(&stable->value)` — decodes it and applies the modify as a full update). This path exists because on a leader the stable btree stores all values including encoded tombstones (since the leader writes directly to stable). No test triggers this path: `test_layered_modify01` exercises the follower path only (via checkpoint pickup), and leader-side modify tests in `test_layered22` do not set up the delete-encoded-value precondition.

**Code path analysis:**
- `src/cursor/cur_layered.c:__clayered_modify_leader()` — lines 2541–2548, the delete-encoded branch
- Triggered when: a value in stable starts with the tombstone escape sequence (e.g., value was previously tombstone-encoded by a follower write that was then checkpointed and the node stepped up)

---

## Summary Table

| Priority | Gap | Risk |
|---|---|---|
| HIGH | `cursor.reserve()` commit path (reserve+write+commit) untested (CW-H7) | Write-after-reserve unverified; concurrent lock semantics not validated |
| CRITICAL | `search_near` dual-iterate recovery (NEXT→PREV both exhausted) cursor state correctness | Wrong neighbor returned, violates `search_near` contract |
| CRITICAL | `cursor.modify()` on ingest tombstone path | Data corruption: delta applied to tombstone bytes instead of empty base |
| HIGH | `cursor.update()` with `overwrite=false` on tombstoned or stable-only key | Phantom re-insertion or false `WT_NOTFOUND` |
| HIGH | `largest_key()` returns deleted key (tombstone as largest ingest key) | MongoDB range tracking includes phantom key |
| HIGH | `cursor.bound()` propagation to new stable cursor after checkpoint advance mid-iteration | Keys outside declared range returned; range scan produces incorrect results |
| HIGH | `next_random` with combined stable+ingest data (tombstone resolution path) | Random cursor returns deleted key or fails assertion |
| HIGH | Positioned `cursor.remove()` during active `next/prev` iteration | Iterator skips a key or revisits removed key |
| MEDIUM/DEFERRED | Step-down mid-transaction `WT_ROLLBACK` enforcement (SD-4; requires elegant step-down) | Leader transaction proceeds as follower, stable btree corrupted |
| SUPERSEDED | `cursor.bound()` + `cursor.search()` interaction — covered by `test_layered82.py` | — |
| SUPERSEDED | `cursor.reset()` clears bounds — covered by `test_layered82.py` | — |
| MEDIUM | Concurrent cursors: one mid-scan, one modifying (read-committed) | Non-repeatable read or missed key at read-committed isolation |
| MEDIUM | Tombstone added to ingest mid-iteration covering stable key (read-committed) | Deleted key returned in scan results |
| LOW | `cursor.compare()` with custom collators, and cross-cursor EINVAL path | Incorrect sort order comparison, no test validates error path |
| LOW | `cursor.modify()` leader path on delete-encoded stable value | Modify applied to wrong base; partial duplicate of follower path |
