# Gap Analysis: Layered Cursor / Layered Table Operations
Generated: 2026-05-06

Source tickets reviewed: 19 (from `/tmp/agent_g2.txt`)
Jira tickets queried: WT-14492, WT-14543, WT-14545, WT-14563, WT-14806, WT-14895, WT-15189, WT-15411, WT-15970, WT-16810, WT-17141, WT-17174, WT-17247, WT-17263, WT-17343.

---

## Confirmed Testing Gaps

### [WT-17247] — Layered cursor writes on follower do not check stable cell's full time window

**Type/Priority/Status:** Bug / Critical P2 / Open  
**Jira context:** Follower `remove()`, `insert()` (no-overwrite mode), and `update()`/`modify()` on the follower decide "does this key have a live value?" by asking the stable cursor at `session->read_ts`. A committed `stop_ts > read_ts` on the stable cell is invisible to the session but visible to the drain — producing inconsistent ingest/drain state. The drain assert `__layered_assert_tombstone_has_value_on_stable_btree` fires when this inconsistency is encountered. The last Jira comment explicitly calls out that a test for WT-17278 should be added when the fix lands.  
**Gap:** No Python test exercises the specific scenario: write key K with stop_ts S on leader → checkpoint → step-down → follower reads at read_ts R < S (stable stop invisible) → follower writes tombstone/update to ingest → drain fires assert. The WT-17247 description identifies three affected write paths (remove, insert no-overwrite, update/modify-follower) but the current test suite only exercises the basic remove-on-stable-only path via test_layered93, which does not vary timestamps to produce the invisible-stop-ts scenario.  
**Suggested test:** Parameterized test across (remove / insert-no-overwrite / update / modify) × (read_ts < stop_ts / read_ts == stop_ts) on follower. Requires `preserve_prepared=true` and `disagg.mode=switch` to reach the triggering scenario. Verify that the drain completes cleanly or that the correct `WT_NOTFOUND` / `WT_ROLLBACK` is returned before any write lands in ingest.  
**Already in existing analysis?** No — NEW GAP (CW-H8)

---

### [WT-15970] — Layered cursors to wait for ingest table to drain during step-up

**Type/Priority/Status:** Bug / Major P3 / Open  
**Jira context:** When a follower steps up to leader, `__clayered_enter` closes the ingest cursor immediately via `__clayered_open_stable`. This is only safe if the ingest table is fully drained. However `WT_CONN_RECONFIGURING_STEP_UP` is set while step-up is in progress and drain may not be complete. If cursor operations on a positioned layered cursor straddle the drain completion window, the ingest half of the merge view may be silently dropped. Multiple `FIXME-WT-16810` comments in `cur_layered.c` (lines 697, 958, 1678) note that ingest-skip logic "will need revisiting when asynchronous step-up is supported".  
**Gap:** No test exercises: hold a positioned layered cursor on the follower → initiate step-up while ingest is not yet drained → continue iterating the cursor → verify that data visible before step-up is still visible post-step-up. The existing `test_layered77` only checks that step-down (leader→follower) doesn't crash during dirty eviction; it does not check cursor data-visibility invariants across step-up.  
**Suggested test:** Open a cursor on a follower with data in both ingest (follower writes) and stable (leader checkpoint). Begin iterating. Trigger step-up. Continue iterating. Assert all keys that were visible before step-up remain visible, and assert no key is returned with a stale tombstone/value from the abandoned ingest.  
**Already in existing analysis?** No — NEW GAP (CW-H9)

---

### [WT-14806] — Layered cursors tombstone ambiguity

**Type/Priority/Status:** Task / Major P3 / Open  
**Jira context:** The magic tombstone value `{"\x14\x14", 2}` is used in-band to mark deleted records. The ticket notes three under-tested areas explicitly called out in its last comment (Yury Ershov, June 2025):
1. `__clayered_largest_key()` — can return a key with an ingest tombstone value (key exists in stable, deleted in ingest); is this correct? The code at lines 2321–2340 does not filter tombstones before returning the largest key.
2. `__clayered_next_random()` — can return a tombstone if the random pick from stable or ingest lands on a deleted key before `search_near` resolves it. The current flow calls `__clayered_search_near_int` after picking a random key, but does not validate that the resulting key's value after `__clayered_deleted_decode` is non-tombstone.
3. `__clayered_modify()` — a series of modifications that produce a value starting with `\x14\x14` would be silently treated as a tombstone. `modify()` was not ported from LSM (which rejected it), so this path has had no comparable historical exercise.  
**Gap:** (a) No test for `largest_key()` when the largest key has been deleted in ingest but still exists in stable — the return value is ambiguous. (b) No test verifying that `next_random()` on an ingest table where the majority of rows are tombstoned still returns a valid non-deleted row (existing CI timeout WT-15189 shows this is a real scenario). (c) No test for `modify()` producing a value that collides with the tombstone sentinel.  
**Suggested tests:** (a) Insert K=max in leader, checkpoint, delete K in follower ingest, call `largest_key()` on follower — should return K-1 (the true largest visible key), not K. (b) Fill ingest with 1000 rows, delete 999 of them, call `next_random()` N times, assert non-tombstone values are returned. (c) `modify()` the first two bytes of a value to `\x14\x14`, read back and assert the value is not interpreted as a deletion.  
**Already in existing analysis?** Partially — CR-H3 covers `search_near` on tombstoned exact-match but does NOT cover `largest_key`, `next_random`, or `modify` tombstone collision. The three sub-items are NEW GAPS (CQ-H1, CQ-H2, CQ-H3).

---

### [WT-14563] — Bulk load for layered cursors not supported (returns EINVAL)

**Type/Priority/Status:** Bug / Major P3 / Open  
**Jira context:** `__clayered_open` at line 2754–2756 returns `EINVAL` immediately if `bulk=true` is requested. The ticket describes a planned implementation: leader mode → bulk to stable; follower mode → bulk to ingest; role-transition during bulk → return `WT_ROLLBACK`. The ticket is open and unassigned with no implementation PR.  
**Gap:** No test verifies that `open_cursor(..., "bulk=true")` on a `layered:` table returns the correct error, no test verifies any partial bulk behaviors that may exist, and no test will be ready to validate the eventual implementation. The test suite has zero bulk coverage for layered cursors (confirmed by grep — only `test_layered_fast_truncate03.py` mentions "bulk page reads" in comments, not bulk cursors).  
**Suggested test:** (a) Immediate gap: test that `open_cursor(..., "bulk=true")` currently returns `EINVAL` on a layered table so any unintended behavior change is caught. (b) Once WT-14563 implementation lands: test bulk load on leader (bulk→stable), bulk load on follower (bulk→ingest), and role-transition-during-bulk returning expected error.  
**Already in existing analysis?** No — NEW GAP (CW-H10)

---

### [WT-14545] — Layered cursors step-down (leader→follower) with open cursors

**Type/Priority/Status:** Improvement / Major P3 / Open  
**Jira context:** `test_layered31.py` has a section (lines 264–284) explicitly guarded `if False:` with comment `# FIXME-WT-14545: enable this test when stepping down is debugged.` This section tests that a cursor held across a leader→follower step-down retains correct position and that subsequent writes produce `WT_CONFLICT`. The ticket (assigned, sprint 2026-06-05) tracks making step-down work correctly. Sid Mahajan's last comment confirms `test_layered31` is the gating test.  
**Gap:** The gated code covers only sequential cursor position preservation after step-down. It does NOT cover: positioned cursors mid-`next()`/`prev()` scan that are interrupted by step-down, write cursors in a prepared but uncommitted transaction at step-down time, or `search_near` on a cursor that straddles the step-down boundary.  
**Suggested test:** When WT-14545 is fixed, enable the `if False:` block and add: (a) `prev()` scan across step-down; (b) `search_near()` across step-down; (c) rollback of a write transaction that was open at step-down.  
**Already in existing analysis?** CR-H6 covers `cursor.bound()` + step_up. The step-DOWN cursor scenario with positioned cursors mid-scan is a NEW GAP (CR-H7).

---

### [WT-14543] — Oplog emulation: cursors held open across checkpoint pick-up

**Type/Priority/Status:** Improvement / Major P3 / Open (unassigned)  
**Jira context:** `test_layered23.py` contains an `Oplog` class. WT-14543 tracks: (1) making the `Oplog` class reusable across tests, (2) enhancing it to keep cursors open across checkpoint pick-up, (3) writing new tests using enhanced `Oplog` with multiple btrees. The gap is that current oplog tests do not exercise cursor-open-during-checkpoint-pick-up, which is a distinct correctness surface for the layered merge cursor (stable cursor must be re-pointed to the new stable btree while ingest cursor stays open).  
**Gap:** No test holds a layered cursor open, advances the stable checkpoint on the follower, and then continues iterating through the same cursor, verifying that (a) new data from the advanced checkpoint is visible, (b) previously-seen ingest data is still correctly merged, and (c) tombstones from a previous checkpoint interval are correctly handled.  
**Suggested test:** Open layered cursor on follower. Iterate to position P. Leader advances checkpoint (adds new keys). Follower picks up checkpoint. Continue iterating from P. Verify all new keys are visible and no previously-seen key is duplicated.  
**Already in existing analysis?** No — NEW GAP (CR-H8)

---

### [WT-15411] — `positioned` variable correctness in `clayered->remove()`

**Type/Priority/Status:** Bug / Major P3 / Open (unassigned, no description in Jira — only title)  
**Jira context:** The ticket was created to investigate whether the `positioned` variable computed from `F_ISSET(cursor, WT_CURSTD_KEY_INT)` at line 2194 of `cur_layered.c` is used correctly throughout `__clayered_remove()`. A positioned remove takes a different code path (`__clayered_remove_leader` / `__clayered_remove_follower` with `positioned=true`) versus an unpositioned one. Incorrect `positioned` state could cause a remove to skip the key-existence check entirely.  
**Gap:** No targeted test exercises remove on a key where the cursor's `positioned` flag is in an ambiguous state — e.g., after a `search_near()` that returned `exact=-1` (landed on a neighbor, not the target), or after a `next()`/`prev()` that stopped exactly at the key to be deleted. These positions set `WT_CURSTD_KEY_INT` but the cursor is not "positioned on the remove target" in the application intent sense.  
**Suggested test:** (a) `search_near` → land on neighbor (exact=-1) → `remove()` without re-setting key → assert `WT_NOTFOUND` or that the correct key is removed. (b) `next()` to position → `remove()` of that key → verify key gone from both layers. (c) Same as (b) on follower (stable-only key).  
**Already in existing analysis?** CW-H3 covers remove on stable-only key without prior positioning. The positioned-flag-ambiguity scenario is a NEW GAP (CW-H11).

---

### [WT-17174] — Incorrect `read_only=true` / `readonly=true` configuration by layered cursors

**Type/Priority/Status:** Bug / Major P3 / Open (assigned D. Anderson)  
**Jira context:** Layered cursors open the stable constituent with `read_only=true` (wrong config string — should be `readonly=true`). Additionally, readonly cursors are not cached, causing repeated open/close overhead. The active PR discussion (Luke Pearson comment, April 2026) concerns whether non-blind writes need uncached cursor opens after stable-table replacement.  
**Gap:** No test opens a layered cursor with `readonly=true` config from the application side and verifies it is propagated to constituents correctly. No test verifies cursor cache hit rate is non-zero for a follow-up workload of repeated readonly lookups (i.e., that caching works after the fix).  
**Suggested test:** (a) Open a layered cursor with `readonly=true`, perform a series of `search()` operations, close cursor, reopen, repeat — assert that the connection-level `cursor_reuse_count` stat increases. (b) Try to call a write op (`insert`, `update`, `remove`) through a `readonly=true` layered cursor and assert `EACCES`.  
**Already in existing analysis?** No — NEW GAP (CR-H9). (Note: `test_layered88.py` tests `readonly=true` at connection level, not cursor level.)

---

### [WT-17141] — Unreachable code in `__clayered_reserve` (reserve not implemented)

**Type/Priority/Status:** Technical Debt / Major P3 / Open  
**Jira context:** `__clayered_reserve` at line 2077 calls `WT_ERR_MSG(session, ENOTSUP, "Reserve is not currently supported for layered cursors")` — all code after line 2077 in the function is structurally dead (Coverity CID 204067). The `__clayered_reserve_constituent` helper function exists (lines 1864–1882) and is called from `__clayered_reserve_constituents` (lines 1900–1928) suggesting a partial implementation was started.  
**Gap (note this is related to CW-H7):** `test_layered92.py` exists and tests `reserve()` on both leader and follower sides with keys in stable-only, ingest-only, both, and missing states. However, the existing test at line 70 always calls `rollback_transaction()` — it never commits after a successful `reserve()`. The actual use case of `reserve` is to atomically claim a key and then write a value in the same transaction; no test covers the commit path. Additionally, since `reserve` returns `ENOTSUP` currently, all the reserve tests in test_layered92 are calling the constituent reserve path — it is unclear whether these tests run against the full layered cursor dispatch or bypass to constituent directly.  
**Suggested test:** Once `reserve` is implemented for layered cursors: test `reserve()` → `set_value()` → `commit_transaction()` cycle for (a) key in stable only; (b) key in ingest only; (c) key in both; (d) key not present (should fail). Verify committed value is readable after commit.  
**Already in existing analysis?** CW-H7 identifies reserve end-to-end (commit path) as a HIGH gap. WT-17141 confirms the unreachable code and the current `ENOTSUP` return — this is already captured in the analysis as CW-H7. The gap here is specifically about the Coverity dead-code — not a new testing gap beyond CW-H7.

---

### [WT-15058] — `session->ncursors` behaviour within clayered cursors

**Type/Priority/Status:** Task / Major P3 / Open  
**Jira context:** Two `FIXME-WT-15058` comments in `cur_layered.c` (lines 124 and 966) describe: (1) read-committed isolation expects `session->ncursors` to be consistent, but layered cursors open/close internal constituent cursors without updating `session->ncursors` through the normal path; (2) both constituents are expected to be initialized but there is a known issue under read-committed isolation.  
**Gap:** No test exercises a layered cursor under `isolation=read-committed`. All existing layered tests use snapshot isolation (the default). Correctness of read-committed isolation with the layered merge cursor — especially around the `ncursors` accounting and constituent initialization race — is completely untested.  
**Suggested test:** Open a layered cursor with `isolation=read-committed` session. Perform `next()` iteration, `search()`, `search_near()`, and write operations. Verify correct results and no assertion fires on `ncursors`.  
**Already in existing analysis?** No — NEW GAP (CR-H10)

---

### [WT-15189] — `next_random` timeout: all-tombstoned ingest causes infinite loop

**Type/Priority/Status:** Build Failure / Major P3 / Open (active CI failure, open PR)  
**Jira context:** When `next_random=true` is configured on a layered cursor and the ingest table is entirely composed of tombstones, `__clayered_next_random` calls `__wti_curfile_next_random` on the constituent ingest cursor, then calls `__clayered_search_near_int` to resolve tombstones. However, if the stable table returns `WT_NOTFOUND` (empty), the code falls through to use ingest, and `__clayered_search_near_int` finds all-tombstoned ingest and may loop. WT-17343 documents the file-cursor layering violation: constituent cursors are opened without `next_random=true` to avoid breaking `__clayered_iterate`'s deletion-skip loop, but this creates an architectural split.  
**Gap:** No test exercises `next_random()` on a layered cursor where (a) stable is empty and ingest contains only tombstones, or (b) stable has a random sample that lands exclusively on tombstoned-in-ingest keys. The timeout in CI is the symptom; no targeted test confirms either the bug or the fix.  
**Suggested test:** Create a layered table. Write 1000 keys to ingest. Delete all 1000 (leaving tombstones). Call `next_random()` — expect `WT_NOTFOUND` within a bounded number of retries, not a spin. Also test: stable has 100 keys, ingest has tombstones for 90 of them; call `next_random()` 100 times; assert all returned keys are from the 10 non-tombstoned set.  
**Already in existing analysis?** No — NEW GAP (CR-H11). (Related to CQ-H2 from WT-14806 but WT-15189 is an active CI blocker specifically about the spin.)

---

## Uncertain Cases

### [WT-16810] — Layered cursor invariants under disagg leader promotion

**Type/Priority/Status:** Improvement / Major P3 / Backlog  
**Jira context:** `if (leader)` guards were added in `__clayered_get_current`, `__clayered_iterate_constituents`, and `__clayered_search_near` to skip ingest in leader mode (fixing WT-16695). This will break when async draining is enabled (ingest may be non-empty during leader mode). Three `FIXME-WT-16810` markers in cur_layered.c at lines 697, 958, 1678 directly track this.  
**Gap assessment:** The testing gap here is contingent on async draining being enabled. No tests currently stress leader-mode cursor behavior when ingest is non-empty (because that scenario is currently prevented by the if-leader guards). Once async draining is available, all cursor ops in leader mode with non-empty ingest become a new testing surface. Marking as UNCERTAIN because the feature prerequisite (async draining) is not yet landed.  
**Suggested test:** Once async draining is enabled: populate ingest on follower → step-up without waiting for drain → iterate layered cursor on leader → assert correct merge behavior with non-empty ingest.

### [WT-14895] — `__clayered_lookup` + `__clayered_put` redundant traversal

**Type/Priority/Status:** Improvement / Major P3 / Backlog  
**Jira context:** For `update()` in non-overwrite mode, the code does a full `__clayered_lookup` (traverses both layers) and then `__clayered_put` (traverses again for the write). This double-traversal is a correctness surface if a concurrent modification happens between lookup and put, or if the lookup lands on ingest while put goes to stable (leader mode).  
**Gap assessment:** The concurrency window between lookup and put is very narrow in single-session use. However, multi-session concurrent updates where S1 deletes a key between S2's lookup and S2's put is not tested. This is related to CR-H5 (concurrent reader + writer). Marking as UNCERTAIN — may be covered implicitly by format stress tests; needs verification.

---

## No Gap (notes)

- **WT-14492** (optimization review): This is a code-quality / performance task, not a correctness gap. No test gap.
- **WT-14540** (`__wt_clayered_deleted` module boundary): Refactoring task. No test gap.
- **WT-15545** (ingest/stable statistic counting): Statistic correctness. Low testing priority for functional gap analysis.
- **WT-17263** (refactor `__clayered_search_near_int`): Pure readability refactor, explicitly states "no behavior change". No new test surface.
- **WT-17343** (refactor `next_random` file-cursor violation): Structural refactor. The layering violation it documents is the root cause of WT-15189 (captured above as CR-H11). The refactor itself does not introduce new test surface beyond what CR-H11 covers.
- **WT-17131** (follower layered cursor stable table reopen optimization): Performance optimization; existing correctness tests cover this scenario.

---

## New Testing Areas Summary

| ID | Ticket | Operation | Priority |
|----|--------|-----------|----------|
| CW-H8 | WT-17247 | follower write (remove/insert/update/modify) when stable stop_ts > read_ts | CRITICAL |
| CW-H9 | WT-15970 | positioned cursor across step-up while ingest not yet drained | HIGH |
| CW-H10 | WT-14563 | bulk cursor open on layered table (EINVAL now, implementation later) | HIGH |
| CW-H11 | WT-15411 | remove() with ambiguous `positioned` flag (after search_near returning neighbor) | HIGH |
| CQ-H1 | WT-14806 | `largest_key()` when largest key is tombstoned in ingest | HIGH |
| CQ-H2 | WT-14806 | `next_random()` on mostly-tombstoned ingest returns valid row | HIGH |
| CQ-H3 | WT-14806 | `modify()` producing value that starts with tombstone sentinel `\x14\x14` | HIGH |
| CR-H7 | WT-14545 | mid-scan cursor across leader→follower step-down | HIGH |
| CR-H8 | WT-14543 | cursor held open across follower checkpoint pick-up (stable btree re-point) | MEDIUM |
| CR-H9 | WT-17174 | `readonly=true` cursor-level config propagated correctly + cursor caching | MEDIUM |
| CR-H10 | WT-15058 | layered cursor under `isolation=read-committed` | MEDIUM |
| CR-H11 | WT-15189 | `next_random()` spin when all-tombstoned ingest (active CI failure) | HIGH |
