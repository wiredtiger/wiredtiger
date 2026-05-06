# DisAgg Testing Gap Analysis — Master Synthesis
Generated: 2026-05-06  
Sources: 8 agent findings files in `disagg-analysis/findings/`, 987 Jira tickets (WT Open/Backlog, Jan 2025–May 2026), source FIXME scan, `test/analysis/05_scenario_analysis/`

---

## 1. Executive Summary

Analysis of 987 open WiredTiger Jira tickets (389 DisAgg-related, 598 other) and their associated Jira context, code FIXMEs, linked bugs, and reproducer scripts produced the following:

| Category | Count |
|---|---|
| New confirmed DisAgg gaps | **101** |
| New confirmed non-DisAgg gaps | **15** |
| Source-code FIXMEs with testing implications | **17** |
| Disabled/skipped tests requiring re-evaluation | **8** |
| Uncertain cases (may or may not be covered) | **28** |
| **Total new gaps not in prior `test/analysis/` synthesis** | **~120** |

The existing `test/analysis/05_scenario_analysis/00_synthesis.md` documented 110+ gaps across 5 analysis passes. This document adds ~120 new ones discovered from Jira context + code analysis.

### Most important findings

1. **Structural gap (WT-15227):** The Python hook (`hook_disagg.py`) never sets `precise_checkpoint=true`. Most disagg Python tests run without the core production constraint. This is the single highest-leverage fix: it transforms every existing disagg test into a stronger test.

2. **Active CI failure (WT-15189 / CR-H11):** `next_random()` hangs on all-tombstoned ingest. An open PR exists; the gap is that no targeted regression test covers the scenario.

3. **Active Critical P2 bug (WT-17247 / CW-H8):** Follower write operations (remove, insert-no-overwrite, update, modify) don't check the full time window on stable cells. Drain assertions fire. No test covers the specific timestamp-visibility scenario.

4. **Publish API cluster (WT-17087/88/89/90/91):** A completely new `WT_SESSION::publish()` API is being implemented with zero test planning to date. This is an entire new API surface.

5. **RTO violation (WT-17352):** Checkpoint pickup at 250k tables takes 27+ minutes. The SLA from WT-14413 requires <15 minutes. No automated regression test exists.

6. **HS verification disabled (FIXME-WT-10779, bt_vrfy.c:1267):** History store validation is completely bypassed in disagg verify path. Any HS corruption in disagg is undetectable by `session.verify()`.

7. **test_corrupt01.py entirely disabled for disagg (FIXME-WT-15064):** All WiredTiger corruption detection tests are skipped under the disagg hook. There is zero corruption testing for layered tables.

---

## 2. Critical Priority DisAgg Gaps

### [CW-H8] — WT-17247: Follower writes ignore stable cell's full time window
**Source:** `findings/02_layered_cursor.md`, `findings/06_validation_verification_testing.md`  
**Type/Priority:** Bug / Critical P2 / Open (sprint 2026-05-22, tagged `expedite`)  
**Description:** `__clayered_remove_follower`, `__clayered_insert` (no-overwrite), and `__clayered_update`/`modify` on the follower check key existence using `session->read_ts` visibility. A stable cell with `stop_ts > read_ts` (committed but invisible at `read_ts`) is not visible to the session but IS honored by the drain — creating an ingest/drain state mismatch. The drain assertion `__layered_assert_tombstone_has_value_on_stable_btree` fires.  
**Gap:** No test specifically exercises the three affected write paths (remove, insert-no-overwrite, update/modify) when `stable.stop_ts > session->read_ts`. The `test_layered93` doesn't vary timestamps to produce the invisible-stop-ts scenario.  
**Suggested test:** Parameterized test: (remove / insert-no-overwrite / update / modify) × (read_ts < stop_ts / read_ts == stop_ts) on follower. Requires `preserve_prepared=true` and `disagg.mode=switch`. Verify drain assertion never fires after fix.  
**Existing analysis?** No — NEW GAP

---

## 3. High Priority DisAgg Gaps

### [CR-H11] — WT-15189: `next_random()` infinite spin on all-tombstoned ingest
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Build Failure / Major P3 / Open (active CI failure, open PR)  
**Description:** When `next_random=true` is configured and the ingest table is entirely tombstoned, `__clayered_next_random` can spin indefinitely because `__clayered_search_near_int` finds only tombstones in ingest and no entries in stable.  
**Gap:** No test exercises: (a) stable empty + ingest all-tombstoned = should return `WT_NOTFOUND` in bounded retries; (b) stable has 100 keys, ingest tombstones for 90 of them — `next_random` should return from the non-tombstoned 10.  
**Suggested test:** Write 1000 keys to ingest, delete all 1000, call `next_random()` — expect `WT_NOTFOUND` within bounded time.  
**Existing analysis?** No — NEW GAP

---

### [WT-17278] — Follower remove: WT_NOTFOUND where leader returns WT_ROLLBACK
**Source:** `findings/06_validation_verification_testing.md`  
**Type/Priority:** Bug / P3 / Open (sprint 2026-06-05)  
**Description:** In multi-node predictable replay, the leader's `__wt_btcur_remove` returns `WT_ROLLBACK` (invisible committed update above visible tombstone). The follower's `__clayered_remove_follower` calls `__clayered_lookup`, sees only the tombstone, returns `WT_NOTFOUND`. Hash mismatch detected. Python reproducer posted in Jira comments but not committed.  
**Gap:** Follower remove path doesn't detect invisible committed updates above visible tombstones. Reproducer script is not in the test suite.  
**Suggested test:** Insert key at ts=100, tombstone at ts=200, re-insert at ts=300, checkpoint. On follower at read_ts=250 call `cursor.remove()`. Before fix: `WT_NOTFOUND`. After fix: `WT_ROLLBACK`.  
**Existing analysis?** No — NEW GAP

---

### [WT-15064] — Corruption detection tests for disagg shared table pages
**Source:** `findings/06_validation_verification_testing.md`  
**Type/Priority:** Task / P3 / Open — unassigned; 5 commits in dev history  
**Description:** Standard corruption tests (write invalid bytes to local `.wt` files) don't work for shared tables in PALI/PALM. Two approaches proposed: (1) PALM Python wrapper; (2) dedicated C testing API. Neither implemented.  
**Gap:** `test_corrupt01.py` is entirely disabled for disagg. No test verifies that `session.verify()` catches corruption in a disagg shared table page.  
**Suggested test:** Use PALM Python wrapper to zero-fill a data page for a known key; call `session.verify()` and assert WT_ERROR + meaningful message. Run on both leader and follower.  
**Existing analysis?** No — NEW GAP

---

### [WT-17087/88/89/90/91] — Publish API: zero test coverage for entire new API
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Tasks / Major P3 / Open (assigned P. Macko)  
**Description:** New `WT_SESSION::publish(uri, epoch)` API controls when schema operations become visible to other nodes. Five sub-tickets: (87) leader-side publishing, (88) assert no writes to unpublished table, (89) follower-side queue management, (90) checkpoint pickup vs local-only table reconciliation, (91) step-down queue preservation. API does not exist yet.  
**Gap:** Zero tests planned for any of these scenarios. Entire new API surface.  
**Key test scenarios:**
- Leader creates table, checkpoints WITHOUT `publish` → table invisible to followers
- Leader calls `publish(epoch)` then checkpoints → table visible to followers
- Follower picks up checkpoint covering schema_epoch → queue pruned
- Step-up replays queued unpublished operations to create stable tables
- Write to unpublished table triggers panic/assert (WT-17088)
- Step-down clears queue correctly (WT-17091)  
**Existing analysis?** No — NEW GAP (entire API cluster)

---

### [WT-16879] — Dhandle-open / step-down TOCTOU race
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Task / Major P3 / Open  
**Description:** Thread A checks `leader==true`, starts building URI without checkpoint suffix, enters `__wt_btree_open()` with no `WT_DHANDLE_OPEN` set. Thread B (step-down) sweeps all `WT_DHANDLE_OPEN` handles — Thread A's is skipped. Thread B sets `conn->leader=false`. Thread A finishes `__wt_btree_open()` setting `WT_DHANDLE_OPEN` — leaving a read-write btree open on a follower.  
**Gap:** No test races btree open against step-down. All step-down tests close cursors/dhandles cleanly first.  
**Suggested test:** Multi-threaded: Thread 1 in tight loop opens new layered tables. Thread 2 calls `conn.reconfigure(role="follower")`. After step-down, verify no btree is open in read-write mode.  
**Existing analysis?** No — NEW GAP

---

### [WT-14949] — Error code when WT API called during reconfigure (step-up/step-down)
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Task / Major P3 / Open (assigned J. Chen)  
**Description:** All API calls should return a defined error (`EBUSY`/`EINVAL`) while `WT_CONN_RECONFIGURING_STEP_UP` is set, instead of crashing. Future: read cursors/transactions may be allowed to stay open.  
**Gap:** No test calls any WT API (e.g., `cursor.insert()`, `session.begin_transaction()`) while step-up is in progress and asserts a non-crash error code.  
**Suggested test:** Thread 1: `conn.reconfigure(role="leader")`. Thread 2: immediately after, call `cursor.insert()`. Assert Thread 2 receives defined error code. After step-up completes, assert normal operations resume.  
**Existing analysis?** Related to existing Gap 5 (open transactions during promotion), error-code dimension is NEW

---

### [WT-15808] — Read cursor survival behavior during step-up
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Task / Major P3 / Open (backlog)  
**Description:** Current behavior when a read cursor is open at step-up time is undefined. The future goal is to allow read-only cursors/transactions to survive step-up.  
**Gap:** (a) No test for what error code a read cursor open during step-up receives; (b) no test for whether cursor is usable after step-up; (c) no test for what data the cursor sees post-transition.  
**Suggested test:** Open read cursor on follower, position it, call `conn.reconfigure(role="leader")` while cursor is open, call `cursor.next()` on still-open cursor. Assert defined behavior (specific error or continued follower-checkpoint data).  
**Existing analysis?** Related to existing Gap 5; read-only cursor survival is NEW

---

### [WT-17090] — Checkpoint pickup vs. local-only table reconciliation
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Task / Major P3 / Open (assigned P. Macko)  
**Description:** Two reconciliation cases: (a) table in checkpoint shared metadata but not locally — follower must pick it up; (b) table exists locally but not in checkpoint — follower created it without publishing, must be dropped/queued. `EBUSY` can occur when dropping table during checkpoint pickup.  
**Gap:** No test exercises scenario (b): follower creates table locally, picks up leader checkpoint that doesn't include it (never published), and must detect/handle divergence.  
**Suggested test:** Follower calls `session.create('layered:t2')` locally. Picks up leader checkpoint NOT containing t2. Verify: no crash, t2 is either dropped or inaccessible, subsequent pickups work normally.  
**Existing analysis?** No — NEW GAP

---

### [WT-17309] — Step-up with unreset cursor: no clean-error test
**Source:** `findings/04_follower_leader_roles.md`  
**Type/Priority:** Task / Major P3 / Backlog  
**Description:** Currently all cursors must be reset before step-up; the code panics or asserts if a cursor is open. No test deliberately triggers this to confirm the error is clean (not silent corruption). Relaxed path (cursor survives step-up) needed for WT-15808.  
**Gap:** (a) Negative test: open cursor, don't reset, call step-up, verify defined error (not crash). (b) Once relaxation implemented: positive test verifying cursor continues to function.  
**Existing analysis?** CR-H6 covers `cursor.bound()` + step_up. Explicit error-on-unreset-cursor is NEW

---

### [FT-GC1] — WT-16813: Truncate list GC at follower checkpoint pickup
**Source:** `findings/03_metadata_gc_ingest.md`, `findings/04_follower_leader_roles.md`, `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Type/Priority:** Task / P3 / In Progress (K. Chovhan, sprint 2026-05-08)  
**Description:** Without GC on checkpoint pickup, followers accumulate an ever-growing truncate list, degrading cursor performance. The ticket explicitly requires a functional test. No test exists yet.  
**Gap:** No Python test verifies that truncate-list entries are pruned on checkpoint pickup while active entries remain.  
**Suggested test:** Leader issues 5 truncates across 3 checkpoints. Follower advances through them. After each pickup, assert entries whose stable timestamp is covered are pruned; entries overlapping the current read window remain. Verify data visibility is correct.  
**Existing analysis?** No — NEW GAP

---

### [TT-GC1] — WT-14521: GC safety under pinned transaction IDs
**Source:** `findings/03_metadata_gc_ingest.md`  
**Type/Priority:** Task / P3 / Open (unassigned)  
**Description:** Is it enough to check global visibility of a txn ID before GC-ing an ingest key? During step-up drain, can we skip the check for keys already in stable?  
**Gap:** No test confirms that a GC attempt on an ingest key whose owning txn is NOT yet globally visible correctly retains the key.  
**Suggested test:** Two concurrent readers on follower, one pinning oldest active txn. Advance `oldest_timestamp` to allow GC but keep pinning txn open. Verify ingest key NOT GC'd. Complete drain. Confirm key removed.  
**Existing analysis?** No — NEW GAP

---

### [TT-H3] — WT-16257: Cross-node oldest_timestamp propagation
**Source:** `findings/03_metadata_gc_ingest.md`  
**Type/Priority:** Task / P3 / Backlog  
**Description:** For PIT reads on followers, the follower must know the leader's `oldest_timestamp` at checkpoint time to correctly reject reads older than the leader's GC horizon. Without this, a follower allows reads the leader has already GC'd.  
**Gap:** No test verifies follower correctly uses leader's `oldest_timestamp` from checkpoint metadata to reject read-timestamp transactions.  
**Suggested test:** Leader sets `oldest_timestamp=50`, checkpoints. Follower picks up checkpoint. Follower attempts `begin_transaction(read_timestamp=30)`. Assert `WT_NOTFOUND` or appropriate error.  
**Existing analysis?** TT-H1 covers follower's own oldest_ts; cross-node propagation is NEW

---

### [V-GC1] — WT-14913: ingest↔stable coherence in verify()
**Source:** `findings/03_metadata_gc_ingest.md`  
**Type/Priority:** Task / P3 / Backlog  
**Description:** Extends `session->verify()` to check that every key in ingest either has a matching stable entry or is genuinely new.  
**Gap:** No test exists. Existing SO-M2 calls `verify()` on a table with only ingest data but doesn't check ingest↔stable coherence.  
**Suggested test:** Write through leader checkpoint cycle so some keys are in stable and some in ingest. Follower calls `verify()` — assert passes. Corrupt an ingest entry (value mismatch vs stable) — assert `verify()` returns error.  
**Existing analysis?** Partial (SO-M2 covers basic call, not coherence) — NEW sub-gap

---

### [V-GC2] — WT-15476/17189: GC-time ingest vs stable mismatch detection
**Source:** `findings/03_metadata_gc_ingest.md`, `findings/06_validation_verification_testing.md`, `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Epic + child tasks / P3 / Open (Jasmine Bi, due 2026-05-15)  
**Description:** At GC time, verify that the most-recent-update about to be pruned exists in stable (for updates) or doesn't exist (for tombstones). WT-17189 (debug build), WT-17190 (HS check), WT-17192 (release-build probabilistic). Active implementation; several failure modes found during PR review.  
**Gap:** No test deliberately introduces ingest-vs-stable mismatch and asserts the GC verify fires. No test for HS cross-check. No test for probabilistic sampling rate.  
**Suggested test:** Write key K on leader, checkpoint to stable. On follower, remove K from stable (fault injection). Trigger GC. Assert panic/WT_ERROR in debug build. Separately: verify sampling rate stat matches expected 1-in-N frequency.  
**Existing analysis?** Referenced in SO-H4/H5; debug/HS/release specifics are NEW

---

### [V-SM1] — WT-17146: local↔shared metadata consistency in verify()
**Source:** `findings/03_metadata_gc_ingest.md`, `findings/06_validation_verification_testing.md`  
**Type/Priority:** Task / P3 / Backlog  
**Description:** In disagg mode, table metadata lives in both local `WiredTiger.wt` and shared `WiredTigerShared.wt_stable`. The verify path doesn't cross-check these sources.  
**Gap:** No test calls `verify()` in a way that would expose local↔shared metadata divergence.  
**Suggested test:** Manually insert an orphan entry into `WiredTigerShared.wt_stable` via metadata cursor, call `session.verify()`, assert WT_ERROR.  
**Existing analysis?** No — NEW GAP

---

### [CP-SCALE] — WT-16188/17307/17352: Checkpoint pickup scale violates RTO SLA
**Source:** `findings/03_metadata_gc_ingest.md`, `findings/04_follower_leader_roles.md`, `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Task + Epic / P3 / Open  
**Description:** Checkpoint pickup iterates the entire shared metadata table O(N) per pickup. Real-world case: 52-minute startup (HELP-88868). At 250k tables: 27+ minutes, violating the 15-min RTO SLA from WT-14413. Fix: lazy ingest table creation.  
**Gap:** No performance test or scale test. `test_layered29` creates 10k tables with no data and no latency assertion.  
**Suggested test:** Create 500+ layered tables on leader, checkpoint, measure follower pickup time. Assert < N seconds. Characterize scaling curve for N=1k/10k/100k. Add as CI regression detector.  
**Existing analysis?** CP-H1 covers "before first checkpoint" scenario; scale is NEW

---

### [CW-H9] — WT-15970: Positioned cursor across step-up while ingest not yet drained
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Bug / Major P3 / Open  
**Description:** `__clayered_enter` closes the ingest cursor immediately during step-up. This is only safe when drain is complete. `WT_CONN_RECONFIGURING_STEP_UP` flag should gate this. Three `FIXME-WT-16810` comments in `cur_layered.c` lines 697, 958, 1678.  
**Gap:** No test exercises: hold positioned layered cursor on follower → initiate step-up while ingest not drained → continue iterating → verify data visible before step-up is still visible.  
**Suggested test:** Open cursor on follower with data in both ingest (follower writes) and stable (leader checkpoint). Begin iterating. Trigger step-up. Continue iterating. Assert all pre-step-up keys remain visible.  
**Existing analysis?** No — NEW GAP

---

### [CW-H10] — WT-14563: Bulk cursor open on layered table returns EINVAL (no test)
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Bug / Major P3 / Open (unassigned)  
**Description:** `__clayered_open` returns `EINVAL` immediately if `bulk=true`. Planned implementation: leader → bulk to stable; follower → bulk to ingest; role-transition → `WT_ROLLBACK`. No test verifies even the current `EINVAL` behavior.  
**Gap:** Zero bulk coverage for layered cursors in the entire test suite.  
**Suggested test:** (a) Immediate: verify `open_cursor(..., "bulk=true")` returns `EINVAL`. (b) Once implemented: bulk load on leader, follower, and role-transition-during-bulk.  
**Existing analysis?** No — NEW GAP

---

### [CW-H11] — WT-15411: `remove()` with ambiguous `positioned` flag
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Bug / Major P3 / Open (unassigned)  
**Description:** The `positioned` variable computed from `F_ISSET(cursor, WT_CURSTD_KEY_INT)` in `__clayered_remove()` may be incorrect when a cursor lands on a neighbor (after `search_near()` with `exact=-1`). Incorrect state could skip the key-existence check.  
**Gap:** No targeted test exercises remove on a key where `positioned` is in an ambiguous state after `search_near()` returning neighbor.  
**Suggested test:** `search_near` → land on neighbor (exact=-1) → `remove()` without re-setting key → assert `WT_NOTFOUND` or correct key removed. Same for `next()` → position → `remove()`.  
**Existing analysis?** CW-H3 covers remove on stable-only key; positioned-flag-ambiguity is NEW

---

### [CQ-H1] — WT-14806: `largest_key()` when largest key is tombstoned in ingest
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Task / Major P3 / Open  
**Description:** `__clayered_largest_key()` lines 2321-2340 don't filter tombstones before returning. A key deleted in ingest but still in stable is returned as largest_key (wrong answer).  
**Gap:** No test for `largest_key()` when the largest key has been deleted in ingest.  
**Suggested test:** Insert K=max in leader, checkpoint, delete K in follower ingest, call `largest_key()` — assert K-1 (true largest), not K.  
**Existing analysis?** CR-H3 covers `search_near` on tombstoned exact-match; `largest_key` is NEW

---

### [CQ-H2] — WT-14806: `next_random()` on mostly-tombstoned ingest returns valid row
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Task / Major P3 / Open  
**Description:** `__clayered_next_random()` can return a tombstone if the random pick lands on a key deleted in ingest. Related to CR-H11 but distinct: this is the general case, not the all-tombstoned spin.  
**Gap:** No test verifying `next_random()` returns valid non-deleted rows when majority of ingest rows are tombstoned.  
**Suggested test:** Fill ingest with 1000 rows, delete 999, call `next_random()` N times, assert all returned values are non-tombstone.  
**Existing analysis?** No — NEW GAP

---

### [CQ-H3] — WT-14806: `modify()` collision with tombstone sentinel `\x14\x14`
**Source:** `findings/02_layered_cursor.md`  
**Type/Priority:** Task / Major P3 / Open  
**Description:** A series of modifications that produce a value starting with `\x14\x14` (the tombstone sentinel) would be silently treated as a deletion. `modify()` was not ported from LSM which rejected it, so there's no historical test coverage.  
**Gap:** No test for `modify()` producing a value that begins with the tombstone sentinel.  
**Suggested test:** `modify()` first two bytes of a value to `\x14\x14`, read back, assert value is not interpreted as deletion.  
**Existing analysis?** No — NEW GAP

---

### [CR-H7] — WT-14545: Mid-scan cursor across leader→follower step-down
**Source:** `findings/02_layered_cursor.md`, `findings/04_follower_leader_roles.md`  
**Type/Priority:** Improvement / Major P3 / Open (assigned D. Anderson, sprint 2026-06-05)  
**Description:** `test_layered31.py` Part 6 (lines 264-284) is guarded `if False:` with comment `# FIXME-WT-14545: enable this test when stepping down is debugged.` The disabled code tests cursor position preservation through step-down.  
**Gap:** No test for: positioned cursor mid-`next()`/`prev()` scan interrupted by step-down; write cursor in prepared-but-uncommitted txn at step-down; `search_near` straddling step-down boundary.  
**Suggested test:** When WT-14545 is fixed, enable the `if False:` block and add: (a) `prev()` scan across step-down; (b) `search_near()` across step-down; (c) rollback of write txn open at step-down.  
**Existing analysis?** CR-H6 covers `cursor.bound()` + step_up; step-DOWN cursor is NEW

---

### [WT-15227] — Python hook does not enable precise checkpoints
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Type/Priority:** Task / Major P3 / Open (Story Points: 8)  
**Description:** `hook_disagg.py` does not set `precise_checkpoint=true`. A large class of existing layered/disagg Python tests run without the core production constraint. Any test that passes today under the hook may silently test a weaker-than-production configuration.  
**Gap:** Structural: many tests that should exercise precise checkpoints don't. This is not a single test gap but a test infrastructure deficiency affecting potentially hundreds of tests.  
**Resolution:** Resolve the hook design (flag argument or restrict to timestamp-using tests as discussed). Add regression coverage confirming all qualifying tests actually enable precise checkpoints.  
**Existing analysis?** No — NEW STRUCTURAL GAP

---

### [WT-14491] — Table drop coordination across leader and follower
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Story / P3 / Backlog  
**Description:** Acceptance criteria explicitly state: (1) dropped table inaccessible on follower after checkpoint pickup; (2) shared metadata must be cleaned up, not just local.  
**Gap:** No test for coordinated table drop across leader + follower.  
**Suggested test:** Drop table on leader → checkpoint → follower picks up → assert follower cursor on dropped table returns `WT_NOTFOUND`. Separately verify shared metadata is clean.  
**Existing analysis?** No — NEW GAP

---

### [WT-15357] — Layered checkpoint cursors (`checkpoint=WiredTigerCheckpoint`)
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Bug / Major P3 / Open  
**Description:** `checkpoint=WiredTigerCheckpoint` is not supported on layered cursors. Many Python tests rely on this. Fix: reuse existing stable cursor code.  
**Gap:** No test for `wt_session.open_cursor(uri, config="checkpoint=WiredTigerCheckpoint")` on a layered table.  
**Suggested test:** Open a layered table checkpoint cursor on both leader and follower. Verify reads return expected snapshot data. Test after role transition.  
**Existing analysis?** No — NEW GAP

---

### [WT-15594] — Timestamp enforcement on layered table writes
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Task / Major P3 / Open  
**Description:** All writes to layered tables must use timestamps. Enforcement asserts not yet added. Without enforcement, step-up correctness cannot be guaranteed.  
**Gap:** No test verifying that a non-timestamped write to a layered table is rejected.  
**Suggested test:** Insert/update a layered table record without commit timestamp. Assert `EINVAL` or assert fires.  
**Existing analysis?** No — NEW GAP

---

### [WT-16494] — Checkpoint order monotonicity across role changes
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Type/Priority:** Bug / Major P3 / Open  
**Description:** Non-atomic local + remote metadata updates create a window where a stepping-up follower might see an older checkpoint than expected. Checkpoint order must be strictly monotonic: N+1 > N.  
**Gap:** No targeted test for checkpoint order monotonicity after step-up.  
**Suggested test:** Leader checkpoint=N → follower picks up N → step-up → verify new checkpoint N+1 > N. Repeat with concurrent writers to increase race likelihood.  
**Existing analysis?** No — NEW GAP

---

## 4. Medium Priority DisAgg Gaps

### [PP-H1] — WT-14555: Byte-level stats not asserted for delta read/write paths
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** `block_byte_read`, `block_byte_write`, `disagg_block_hs_byte_read` are never asserted in any disagg test. Tests assert delta *counts* but never *bytes*. A silent regression in byte accounting is undetectable.  
**Suggested test:** Write known-size payload, generate delta, read back, assert `block_byte_read == full_page_size + delta_size` (not just `full_page_size`). Cover both HS and non-HS paths.

---

### [PP-H2] — WT-14504: Redundant full-page write suppression untested
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** No test verifies that a reconciliation producing an identical page to the previous one is skipped (not re-sent to PALI). `rec_skip_write` stat exists but is not asserted in any disagg test.  
**Suggested test:** Write page, checkpoint, reopen, make no-op "touch", checkpoint again. Assert `rec_skip_write` incremented and `disagg_block_put` did NOT increase.

---

### [PP-H3] — WT-15940: `wt` utility with disagg config on non-disagg database
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** Running `wt -C 'disaggregated=(page_log=palite)'` against a non-disagg database triggers a confusing `unknown page log 'palite'` error. No test uses `self.runWt()` against a non-disagg database with a disagg config string.  
**Suggested test:** `self.runWt(...)` with disagg config against non-disagg dir, assert user-friendly error message (not raw internal error).

---

### [PP-H4] — WT-16442: Delta eligibility for re-split pages
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** Pages that split on previous reconciliation can't generate deltas on re-reconciliation. `FIXME-WT-15709` at `rec_write.c:3332`. No test verifies that re-split pages correctly produce no delta (current restriction) or that the stat `rec_page_delta_rejected_*` accounts for this case.  
**Suggested test:** Force page to split (checkpoint), update it so it splits again, assert `rec_page_delta_leaf == 0` for that page. (Test the contractual restriction, invert when feature lands.)

---

### [PP-H5] — WT-16239: Full-page selection when majority of page rows are deleted
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** When many globally-visible tombstones exist on a page, writing a full page (omitting deleted entries) saves space vs. a delta with many tombstones. No test verifies the system switches from delta to full-page at a high-delete threshold.  
**Suggested test:** Populate page, delete majority of rows, advance stable_timestamp, checkpoint. Assert full page written (`rec_page_delta_leaf == 0`). Verify PALI response doesn't return stale delta tombstones.

---

### [PP-H6] — WT-16535: `WT_PAGE_LOG_ENCRYPTED` flag correctness per table type
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** Regular user tables must set `WT_PAGE_LOG_ENCRYPTED`; internal bootstrap tables (key provider, turtle file equivalent) must NOT. No test verifies flag is set correctly for all table types. A wrong flag on key provider causes startup failures.  
**Suggested test:** In encryption-enabled build with PALI, create a regular table and a key-provider table. Use PALite instrumentation hook to assert flag presence/absence per table type.

---

### [PP-H7] — WT-15266: All delta-chain entries dumped on checksum failure
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** When a checksum error occurs in a multi-part read (base + N deltas), only the failing page is dumped. Other entries in the results array are lost. No test injects corruption into a delta chain read and verifies all entries are dumped.  
**Suggested test:** Corrupt a specific delta in the chain (flip a byte in PALite stored image), trigger read, assert error message/dump contains hexdump output for ALL entries in results array.

---

### [PP-H8] — WT-15419: PALI error message logging when API call fails
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** When any PALI API call fails, the error code is returned but no human-readable error message is logged. No test verifies a failed PALI call produces a diagnostic log line.  
**Suggested test:** Inject PALite put failure (env var or hook), capture verbose error log, assert message identifies the failed PALI function and table/page affected.

---

### [SM-1] — WT-15591: `WT_IS_METADATA` not covering shared metadata URI
**Source:** `findings/03_metadata_gc_ingest.md`  
**Gap:** Code guards using `WT_IS_METADATA(dhandle)` are incomplete for `WiredTigerShared.wt_stable`. FIXME at `src/conn/conn_layered.c:363`.  
**Suggested test:** Attempt operations known to check `WT_IS_METADATA` (verify, compact, backup) on a disagg connection with the shared metadata table. Assert each either succeeds or returns expected error — doesn't crash.

---

### [SM-2] — WT-16477: dhandle open under concurrent checkpoint pickup (lock contention)
**Source:** `findings/03_metadata_gc_ingest.md`  
**Gap:** Opening a shared table dhandle takes checkpoint lock on follower. FIXME at `src/btree/bt_handle.c:210`. No test exercises: follower session opens a table dhandle while checkpoint pickup is in progress on another thread.  
**Suggested test:** Two threads on follower: one continuously calling `disagg_advance_checkpoint`, other opening and reading a shared table in a loop. Assert no WT_PANIC and every read returns consistent data.

---

### [SM-3] — WT-17040: Shared metadata creation on follower + step-up
**Source:** `findings/03_metadata_gc_ingest.md`  
**Gap:** Followers create the shared metadata table at startup and immediately expire the live dhandle. FIXME at `src/conn/conn_layered.c:1105`. No test: follower starts without pre-existing shared metadata, picks up checkpoint creating it, steps up.  
**Suggested test:** Start follower with no shared metadata in local metadata. Provide `checkpoint_meta` from a leader with multiple tables. Verify pickup correctly creates shared metadata locally. Step up, verify node can write checkpoints.

---

### [SC-1] — WT-17063: Shared disk cache lifecycle at step-up/step-down
**Source:** `findings/03_metadata_gc_ingest.md`  
**Gap:** Shared disk hash table must be initialized on step-up and destroyed/preserved on step-down. No test exercises the hash table lifecycle across role transitions.  
**Suggested test:** Verify hash table is absent in follower mode, created on step-up. After step-down, verify entries are cleaned up. Test conflict detection works correctly post-step-up.

---

### [SC-2] — WT-17250: Shared disk cache integrity validation
**Source:** `findings/03_metadata_gc_ingest.md`, `findings/06_validation_verification_testing.md`  
**Gap:** Ticket explicitly requests a test running WT with shared disk cache for 10-20 minutes, then walking hash table to confirm no dangling entries (refcount=0, or entries referencing evicted pages).  
**Suggested test:** Enable shared cache, run mixed read/write/eviction for N minutes, walk hash table at end, assert no dangling entries.

---

### [CR-H8] — WT-14543: Cursor held open across follower checkpoint pick-up
**Source:** `findings/02_layered_cursor.md`  
**Gap:** No test holds a layered cursor open, advances the stable checkpoint on the follower, and then continues iterating, verifying new data from advanced checkpoint is visible while previously-seen ingest data is still correctly merged.  
**Suggested test:** Open layered cursor, iterate to position P, leader advances checkpoint, follower picks up, continue iterating from P. Verify new keys visible, no previously-seen key duplicated.

---

### [CR-H9] — WT-17174: `readonly=true` cursor config propagation and caching
**Source:** `findings/02_layered_cursor.md`  
**Gap:** Layered cursors open stable constituent with wrong config string (`read_only=true` instead of `readonly=true`). Readonly cursors not cached. No test verifies propagation or that cache hit rate is non-zero for repeated readonly lookups.  
**Suggested test:** Open layered cursor with `readonly=true`, perform repeated searches, close, reopen. Assert `cursor_reuse_count` stat increases. Assert write op via readonly cursor returns `EACCES`.

---

### [CR-H10] — WT-15058: Layered cursor under `isolation=read-committed`
**Source:** `findings/02_layered_cursor.md`  
**Gap:** Two `FIXME-WT-15058` comments in `cur_layered.c` (lines 124, 966) document known issues with read-committed isolation and `session->ncursors` accounting. No test exercises layered cursors under `isolation=read-committed`.  
**Suggested test:** Open layered cursor with `isolation=read-committed`. Perform `next()`, `search()`, `search_near()`, and write ops. Verify correct results, no assertion on `ncursors`.

---

### [WT-14541] — Full API surface in follower mode via extended hook
**Source:** `findings/04_follower_leader_roles.md`  
**Gap:** Almost no Python tests exercise the full standard API surface in follower role. Hook's follower path intentionally skips many tests. Proposed fix: hook's close path calls `reconfigure(role="leader")` and waits for checkpoint before closing.  
**Resolution:** Extend `hook_disagg.py` close path. Enable subset of existing tests to run in follower mode — starting with read-only API (scan, stat query), then schema operations that should return `ENOTSUP`.

---

### [WT-15860] — Internal thread / role transition race
**Source:** `findings/04_follower_leader_roles.md`  
**Gap:** Example race: node is primary, checkpoint cleanup thread finds a table, node steps down, checkpoint cleanup initiates writes — which should not happen. No test exercises internal service threads (stat log server, sweep, checkpoint cleanup) racing with step-up/step-down.  
**Suggested test:** Use `timing_stress_for_test` to slow checkpoint cleanup or sweep server, trigger step-down while they're in flight. Assert no writes initiated by internal thread after step-down.

---

### [WT-17088] — Write to unpublished table should trigger panic
**Source:** `findings/04_follower_leader_roles.md`  
**Gap:** When `__disagg_shared_metadata_op` is called with `WT_SHARED_METADATA_UPDATE` and the table isn't yet in shared metadata, it should panic. No test verifies this assertion fires.  
**Suggested test:** In diagnostic build: create table, immediately call `cursor.insert()`, call `session.checkpoint()` WITHOUT calling `publish`. Verify panic or `WT_ERROR` with message matching "unpublished table".

---

### [WT-17091] — Step-down clears publish queue correctly
**Source:** `findings/04_follower_leader_roles.md`  
**Gap:** Step-down currently clears the metadata operation queue — this is wrong for elegant step-down (queue must be preserved for subsequent step-up replay). No test verifies queue state is preserved vs. cleared appropriately at step-down.  
**Suggested test (current):** Leader creates table T, calls `publish(T, epoch=5)`, does NOT checkpoint. Simulate server restart (step-down). Start new leader with `checkpoint_meta` from last complete checkpoint. Verify T is NOT visible to followers.

---

### [WT-17307] — Checkpoint pickup latency regression with large table counts
**Source:** `findings/04_follower_leader_roles.md`  
**Gap:** Creating 50k+ tables causes standby to lag 2x behind primary. No Python test benchmarks or exercises checkpoint pickup latency with large numbers of tables. `test_layered29` creates 10k tables but has no latency assertion.  
**Suggested test:** Create 5,000 layered tables with 100 rows each, checkpoint, measure follower pickup latency. Assert completion within timeout proportional to table count.

---

### [WT-14497] — Precise checkpoint behavior when no stable timestamp is set
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Policy unresolved: write nothing or write all content? No test covers this. `FIXME-WT-14721` at `conn_api.c:3449` confirms the path has no automated test guard.  
**Suggested test:** Open disagg connection with `precise_checkpoint=true`, issue checkpoint BEFORE any `set_timestamp('stable_timestamp=...')`, assert specific outcome (error or defined snapshot).

---

### [WT-14830] — Stress testing for prepared atomicity under precise checkpoints
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Ticket explicitly calls for adding preserve-prepared testing to `test/format` and `test/checkpoint`. No such stress test exists. Story Points=5, defined pipeline, no PR.  
**Suggested test:** `test/format` with `ops.prepare=1` + `precise_checkpoint=true` in disagg leader mode, concurrent workload interleaving prepares/commits/rollbacks with frequent checkpoints. Verify after crash-recovery no partially-applied prepared txn is visible.

---

### [WT-15397] — Truncate silently disabled when precise_checkpoint + prepare both on
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** `format_config.c:1548` contains `FIXME-WT-15565`: `if (GV(PRECISE_CHECKPOINT) && GV(OPS_PREPARE)) { config_off(NULL, "ops.truncate"); }` — silent combination disabling. The combination `precise_checkpoint + prepare + truncate` is entirely untested.  
**Suggested test:** Once WT-15565 resolved, remove `config_off` guard. Add targeted Python test issuing cursor truncate inside prepared txn in disagg mode.

---

### [WT-15552] — `precise_checkpoint` hardcoded in `test_util.h`
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** `test_util.h` unconditionally forces `precise_checkpoint=true` for any disagg test. Infrastructure cannot distinguish "test explicitly opts in" from "test inherits by accident."  
**Resolution:** Remove hardcode, expose as explicit config option, add validation that fails loudly if not set in disagg mode.

---

### [WT-15294] — `test_prepare20.py` crashes in checkpoint under disagg hook
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** `test_prepare20.py` reliably crashes in `rec_hs.c` during checkpoint reconciliation under `--hook disagg`. The test is on `hook_disagg.fail` blocklist. Prepare-checkpoint interaction exercised by scenario 9 is untested in disagg.  
**Resolution:** Fix the abort, remove from `hook_disagg.fail`, confirm test passes under disagg.

---

### [WT-16259] — Prepared transactions across checkpoint updates on standbys
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Open questions: Do standbys execute prepared transactions? How are in-flight prepares handled when a follower becomes leader? Should prepared txns span checkpoint pick-ups? None tested.  
**Suggested test:** Start prepared txn on leader, checkpoint, step-up follower. Verify prepared txn is correctly handled (rolled back or preserved). Also test `oldest_timestamp` advancing past a prepared-but-uncommitted txn in disagg.

---

### [WT-16732] — No predictable-replay test for truncate in multi-node disagg
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Truncate is not supported in predictable replay. Multi-node `test/format` disagg runs never exercise truncate. Any divergence between leader and follower truncate behavior goes undetected.  
**Resolution:** Extend predictable replay to record and replay truncate operations. Add multi-node format config exercising concurrent truncate with checkpoint under `precise_checkpoint=true`.

---

### [WT-16961] — No test for `best-effort=true` truncate config option
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Ticket acceptance path explicitly requires a Python test for `best-effort=true`. Zero coverage today. Feature not yet implemented.  
**Suggested test:** Issue `session.truncate(uri, start, stop, "best-effort=true")` on a layered table without full fast-truncate support. Verify success returned, partial truncation handled correctly, no corruption.

---

### [WT-17135] — Step-up with pending truncate list entries
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** No test exercises: follower has pending truncate list entries → steps up → new leader processes them correctly.  
**Suggested test:** Issue fast truncates on follower, trigger step-up. Verify truncated ranges not visible on new leader. Verify data outside truncated range intact.

---

### [WT-17377] — `durable_timestamp == prepare_timestamp` should return `WT_EINVAL`
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Ticket explicitly requires a new test covering: `durable_ts == prepare_ts` → `WT_EINVAL`; `durable_ts > prepare_ts` → success.  
**Suggested test:** New `test_prepare37.py` or standalone csuite test covering both rejection and acceptance cases.

---

### [WT-17380] — prepare disabled in disagg switch mode (FIXME in format configs)
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** `ops.prepare=0` hardcoded in all switch-mode format test configurations (FIXME comments in `test/evergreen.yml`). Switch-mode variant never tests prepared transactions.  
**Resolution:** Identify root cause of prepare failures in switch mode. Remove `ops.prepare=0` from switch-mode configs. Confirm switch-mode Evergreen variants run with prepare enabled.

---

### [WT-17188] — btree ID uniqueness in shared metadata (deadlock blocker)
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** WT-17116 added local metadata btree-ID scan. WT-17188 extends to shared metadata. Two blockers: (1) `WT_WITH_CHECKPOINT_LOCK` re-entrancy deadlock when opening checkpoint cursor on shared metadata during verify; (2) PALite infinite-retry `SQLITE_NONE`. PoC PR #13525 blocked.  
**Suggested test:** Inject duplicate btree ID into shared metadata, call `session.verify()`, assert WT_ERROR.

---

### [WT-17125] — `verify()` `read_corrupt=true` mode end-to-end in disagg
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** `bt_vrfy.c` `read_corrupt` mode (lines 977-1048) continues past page-read failures. `block_disagg_read.c:153` has `FIXME-WT-15768: never give up here` — which prevents verify from continuing past errors. No test exercises `read_corrupt=true` in disagg.  
**Suggested test:** PALI fault-injection to make one page return read error. Call `session.verify(uri, "read_corrupt=true")`. Assert: no panic, WT_ERROR returned, remaining pages still verified.

---

### [V-GC3] — WT-17192: Release-build probabilistic GC sampling
**Source:** `findings/03_metadata_gc_ingest.md`  
**Gap:** WT-17192 extends debug-build GC verification to release builds via 1-in-N random sampling. No test design at all. Frequency TBD.  
**Suggested test:** Run 10k GC operations. Confirm sampling rate stat matches expected 1-in-N frequency.

---

### [WT-16148] — Version cursor cannot see orphaned HS entries
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** Key deleted with `use_timestamp=false`, DB reopened, same key re-inserted → old HS entry becomes orphaned. Version cursor on file returns `WT_NOTFOUND` prematurely. Concrete reproducer (`test_hs34`) in ticket, not in test suite.  
**Suggested test:** Commit reproducer from WT-16148 as `test_hs34.py`. Assert both "should see entry" and "correctly reports no entry" cases.

---

### [WT-16136] — Version cursor `stop_durable_ts` ambiguity for HS entries
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** `FIXME-WT-16136` at `cur_version.c:665`. When iterating HS entries with a version cursor, `stop_durable_ts` can come from either a tombstone or a previous full value — code cannot distinguish. No test exercises version cursor over a key with both tombstone and multiple historic HS versions.  
**Suggested test:** Insert key A, update multiple times, evict to HS, open version cursor, assert returned `stop_durable_ts` values are correct for each HS entry (tombstone vs full value).

---

### [WT-14915] — HS verification disabled in disagg via FIXME-WT-10779
**Source:** `findings/06_validation_verification_testing.md`, `findings/08_non_disagg_and_fixmes.md`  
**Gap:** `bt_vrfy.c:1267`: `/* FIXME-WT-10779 - Enable the history store validation. */` — HS verification is completely skipped in disagg verify path. Any HS corruption in disagg is undetectable.  
**Suggested test:** Enable the validation in debug builds for disagg. Add test calling `session.verify()` on a layered table with HS data. Assert no crash. Inject HS corruption. Assert verify catches it.

---

### [WT-16113] — Leader data validation not in main format stress run
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** `format-stress-data-validation-test-disagg-leader` runs as a standalone Evergreen variant, not integrated into `format-stress-test-disagg-leader`. Mirror table validation (leader vs non-layered) only runs in a separate task.  
**Resolution:** Modify format CONFIG.disagg and Evergreen YAML to enable mirror-table comparison at 50% probability within the main disagg stress run.

---

### [WT-15404] — Python tests silently skipped due to `log=` config in disagg
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** Many Python tests set `log=(enabled=true)` even when logging is not the test's purpose. Logged tables unsupported in disagg → auto-skipped, reducing coverage significantly. Actual coverage loss is unquantified.  
**Resolution:** Audit all tests with `log=` configs skipped under disagg. For those where log config is incidental, split into a disagg-compatible variant without `log=`.

---

### [WT-17253] — Sweep/shutdown TSAN data race
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** Sweep server reads sessions that prefetch teardown has already zeroed, causing TSAN data race on shutdown.  
**Suggested test:** Under TSAN build, run repeated open/close of layered table connections with sweep enabled. Assert no data race detected during shutdown.

---

### [WT-17300] — Statistics cursor ENOENT TOCTOU race
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** TOCTOU race in statistics cursor fast path; `ENOENT` (file deleted between stat check and open) not falling back to slow path, leaving statistics incorrect.  
**Suggested test:** Open statistics cursor on layered table while concurrently dropping it. Verify no crash, statistics return `ENOENT` or zero values gracefully.

---

### [WT-17323] — Sweep skips layered dhandles; FD exhaustion at 50k collections
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** Layered dhandle sweeping explicitly skipped in `conn_sweep.c`. At 50k collections, FD exhaustion crashes occur. No test for FD exhaustion under high table count.  
**Suggested test:** Open 50k+ layered tables (or simulate via config). Verify no FD exhaustion crash. Verify that once sweep is re-enabled for layered tables, FDs are released between checkpoint pickups.

---

### [WT-16660/17034] — `bytes_total` accounting leak on `addr_pack` failure
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** Storage accounting leak when `addr_pack` fails mid-write. `bytes_total` not decremented on failure path. No test for `addr_pack` failure path.  
**Suggested test:** Failpoint to inject `addr_pack` failure during checkpoint write. Verify `bytes_total` statistic unchanged after failed write (no leak).

---

### [WT-16044] — Duplicate phylog entries under cache pressure / page splits
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** Under cache pressure, split pages can generate duplicate phylog entries. Partially fixed (WT-16244) but full fix requires delta building for split pages.  
**Suggested test:** High-write-throughput workload on memory-constrained layered table (small cache) to trigger splits. Validate phylog for duplicate entries using diagnostic tooling.

---

### [WT-16627/16663] — Change stream pre-image excluded from disagg suites
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** `change_stream_pre_image_startup_recovery.js` excluded from `no_passthrough_disagg_override` because out-of-order timestamp handling not yet supported in disagg. No disagg-specific recovery test for out-of-order timestamps.  
**Suggested test:** Once WT-16663 resolved, re-enable the test in disagg mode. Add WT-level regression test for out-of-order timestamp during recovery.

---

### [WT-16532/17173] — Eviction walk inefficiency on follower with stale prune_timestamp
**Source:** `findings/07_stories_epics_remaining_disagg.md`  
**Gap:** Eviction walks wasteful between checkpoint pickups on followers because most ingest pages have prune_timestamp newer than what can be evicted. Application threads stall.  
**Suggested test:** Create follower, write data to leader, stall checkpoint pickup on follower. Verify eviction does not stall application threads (or stalls within bounds).

---

## 5. Low Priority DisAgg Gaps

### [PP-L1] — WT-15684: Model tests locked to PALM
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** `test/model` tests hardcode `page_log=palm`. PALI-specific behavior differences (real latency, real error codes, delta interaction with encryption) are untested in model tests.  
**Resolution:** Make page_log setting configurable from command-line. Add model Evergreen task running with `page_log=palite` as minimum viable alternative.

---

### [PP-L2] — WT-16134: test/format locked to PALite
**Source:** `findings/01_pali_page_delta.md`  
**Gap:** PALite has ~5-10% of classic throughput, cannot run parallel jobs. Bugs only manifesting at higher throughput or with concurrent writers are invisible in CI.  
**Resolution:** Add Evergreen tasks running `test/format` with `page_log=pali` and parallelism enabled.

---

### [WT-16118] — Periodic page readback validation on primary (feature-dependent)
**Source:** `findings/06_validation_verification_testing.md`  
**Gap:** No periodic readback catching corruption in transit (primary→LogServer) or at rest. Feature not yet designed.  
**Suggested test (future):** Fault-inject page write in PALM to corrupt checksum, trigger readback validation, assert error surfaced.

---

### [WT-14830] — Prepared atomicity stress test (test/format + test/checkpoint)
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Ticket explicitly calls for adding preserve-prepared stress testing to `test/format` and `test/checkpoint`. No such test exists.

---

### [WT-17330] — Truncate list traversal performance not benchmarked
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** Truncate list is flat linked-list with O(N) traversal. WT-16789 added statistics, but no benchmark confirms stats stay within bounds under load.  
**Suggested test:** Perform N truncate operations, read traversal stats, assert traversal count bounded.

---

### [WT-14361] — `test_truncate16` fast-delete page assertion excluded from disagg
**Source:** `findings/05_precise_ckpt_prepare_rts_truncate.md`  
**Gap:** The test's auto-resolution rule explicitly excludes disagg variants. The same scenario in disagg is untested. Precise checkpoints likely alter fast-delete page count expectations.

---

## 6. Testing Infrastructure Gaps (DisAgg-Specific)

| ID | Ticket | Description | Impact |
|---|---|---|---|
| INFRA-1 | WT-15227 | Python hook missing `precise_checkpoint=true` — most disagg Python tests weaker than production | HIGH — STRUCTURAL |
| INFRA-2 | WT-15552 | `precise_checkpoint` hardcoded in `test_util.h` instead of explicit per-test config | MEDIUM |
| INFRA-3 | WT-15684 | model tests locked to PALM; no PALI or PALite model Evergreen task | MEDIUM |
| INFRA-4 | WT-16134 | `test/format` locked to PALite; PALI-specific bugs invisible in CI | MEDIUM |
| INFRA-5 | WT-14541 | `hook_disagg.py` follower path too aggressive in skipping tests; close path needs leader-flush trick | MEDIUM |
| INFRA-6 | WT-15404 | Unknown number of Python tests silently skipped due to incidental `log=(enabled)` configs | MEDIUM |
| INFRA-7 | WT-16113 | Leader data validation (mirror table comparison) not integrated into main format stress variant | MEDIUM |
| INFRA-8 | WT-16736 | `test/format` disagg multi-node not in all required Evergreen variants | MEDIUM |
| INFRA-9 | WT-17380 | `ops.prepare=0` hardcoded in all switch-mode format configs (FIXME) | MEDIUM |

---

## 7. Source Code FIXMEs with Testing Implications

| Location | FIXME / TODO | Gap |
|---|---|---|
| `src/btree/bt_vrfy.c:1267` | `FIXME-WT-10779 - Enable the history store validation` | HS verification completely skipped in disagg — zero HS corruption detection |
| `src/cursor/cur_version.c:665` | `FIXME-WT-16136: hard to determine if stop durable ts is from tombstone or previous full value` | Version cursor ambiguity for HS entries untested |
| `src/cursor/cur_layered.c:124,966` | `FIXME-WT-15058` — read-committed isolation ncursors accounting | Layered cursor under `isolation=read-committed` never tested |
| `src/reconcile/rec_write.c:3332` | `FIXME-WT-15709: build delta for split pages` | Delta eligibility for re-split pages restriction contractually untested |
| `src/prepared_discover/prepared_discover_walk.c:328` | `TODO: handle prepared fast delete` | Prepared fast truncate not handled during step-up reconstruct; affects step-up correctness |
| `src/block/block_addr.c:111` / WT-15022 | `WT_BLOCK_CHECKPOINT_BUFFER` not sized relative to `WT_ADDR_MAX_COOKIE` | Disagg larger cookies can overflow checkpoint cookie buffer; no test near the limit |
| `src/conn/conn_api.c:3449` | `FIXME-WT-14721: Disagg should enforce precise_checkpoint` | No test for enforcement (or absence of enforcement) of precise_checkpoint in disagg |
| `test/format/format_config.c:1548` | `FIXME-WT-15565: Write prepared truncate operation to disk` | `truncate` silently disabled whenever `precise_checkpoint + prepare` both on in test/format |
| `src/txn/txn_timestamp.c:548` | `FIXME-WT-16310: Check synchronization around oldest/stable timestamps` | Known potential data race on global timestamp fields; no TSAN stress test |
| `src/txn/txn.c:2606` | `FIXME-WT-14739` — shutdown checkpoint for followers | No test for follower shutdown with pending prepared transactions |
| `src/conn/conn_layered.c:363` | `FIXME-WT-14730: check other parts of metadata are identical` | Shared metadata cross-check incomplete |
| `src/conn/conn_layered.c:1105` | `FIXME-WT-17040: investigate if shared metadata creation necessary on follower` | Follower starts, doesn't create shared metadata path untested |
| `src/btree/bt_handle.c:210` | `FIXME-WT-16477` — avoid checkpoint lock via shared metadata read | No test for dhandle open under concurrent checkpoint pickup on follower |
| `src/schema/schema_drop.c:542` | `FIXME-WT-16215: meta_tracking not initialized during recovery` | Drop path during recovery with partial backup restore untested |
| `src/btree/bt_sync_obsolete.c:453` | Read internal pages from non-logged tables when truncate... | Truncate on non-logged tables in disagg may accumulate unreclaimed obsolete pages |
| `src/block_disagg/block_disagg_read.c:153` | `FIXME-WT-15768: never give up on read error` | Prevents `verify read_corrupt=true` from continuing past errors in disagg |
| `src/conn/conn_layered.c:725` | `FIXME-WT-16562` — checkpoint size tech debt | No test validating disagg checkpoint metadata size bounds |

---

## 8. Non-DisAgg Gaps

### [WT-17277] — Prepared fast truncate in `test_checkpoint` and `test_format` (HIGH)
**Gap:** Neither `test_checkpoint` nor `test/format` generates prepared truncations. Entire prepared fast truncate code path (write-to-disk, claim on restart, RTS rollback, crash recovery) exercised only by hand-written unit tests.  
**Resolution:** Ticket acceptance criteria: extend `test_format` to generate prepared truncation ops when prepare enabled; extend `test/checkpoint` to exercise prepared fast truncate under concurrent checkpoint.

---

### [WT-16713] — Victim block cache has zero in-WT test coverage (HIGH)
**Gap:** `test_layered43.py` calls `skipTest("FIXME-WT-15663: currently block cache is disabled.")`. Block cache code (`block_cache.c:874` also has FIXME-WT-15663). All block cache testing is in MongoDB.  
**Suggested test:** Implement mock `WT_PAGE_LOG_HANDLE` in PALite (unordered_map + mutex). Enable via config flag. Enable block cache in test/format and layered Python suite.

---

### [WT-16834/16836] — Table ID uniqueness enforcement (HIGH)
**Gap:** Table ID conflict between key provider table and shared metadata was caught only by MongoDB CI (BF-41795, BF-41785). No WT test reproduces the key-provider vs shared-metadata ID conflict.  
**Suggested test:** (WT-16834) Python/catch2 test creating large numbers of internal + user tables, verifying no ID assigned twice. (WT-16836) Instrument PALite to maintain a set of allocated table IDs and assert uniqueness on each `pl_open_handle`.

---

### [WT-15022] — Checkpoint cookie buffer can overflow with large disagg address cookies (MEDIUM)
**Gap:** `WT_BLOCK_CHECKPOINT_BUFFER` (127 bytes) not sized relative to `WT_ADDR_MAX_COOKIE` (255 bytes). Disagg block manager uses larger cookies and can overflow when many 64-bit integers are added.  
**Suggested test:** catch2 unit test that constructs maximum-sized disagg address cookie and calls pack/unpack checkpoint cookie functions. Assert no overflow. Add C assertion that fires before overflow occurs.

---

### [WT-15061] — Crash point before checkpoint txn commit (MEDIUM)
**Gap:** Existing crash points are at (1) before metadata updates, (2) before metadata sync. No crash point before the checkpoint transaction commit itself. Recovery from crash during commit is untested.  
**Suggested test:** Add `WT_TIMING_STRESS_CHECKPOINT_CRASH_BEFORE_TXN_COMMIT` in `checkpoint_txn.c`. Add to model test's `checkpoint_crash` workload.

---

### [WT-15084] — test/model with WAL-logged tables (MEDIUM)
**Gap:** test/model always runs tables without WAL logging. Logged+non-logged mixed workloads are the real mongod configuration but untested in model.  
**Suggested test:** Add probabilistic `log=(enabled=true)` to a subset of tables in model workload generator. Assert logged tables consistent with WAL and non-logged tables consistent with last checkpoint after crash+recovery.

---

### [WT-15243] — Bulk cursor + drop segfault (MEDIUM)
**Gap:** When bulk cursor has `WT_DHANDLE_EXCLUSIVE`, calling `drop()` from same session incorrectly returns 0 then segfaults during checkpoint-tree. No test opens bulk cursor then attempts `drop()` on same table.  
**Suggested test:** Open bulk cursor on table, do not close it, attempt `session.drop()` on that table URI, assert EBUSY.

---

### [WT-15312] — `drop()` incorrectly returns EBUSY with `WT_UNCOMMITTED_DATA` after all committed (MEDIUM)
**Gap:** Even after all application transactions have committed, `drop()` can return EBUSY. MongoDB has an invariant commented out (SERVER-100890) because of this.  
**Suggested test:** Write committed data, call `drop()` repeatedly. Assert after N retries it succeeds. Assert never returns infinite EBUSY.

---

### [WT-14029] — Timing stress for live restore (MEDIUM)
**Gap:** `WT_TIMING_STRESS_LR_SLOW` doesn't exist. Live restore completes too quickly on small test files to expose concurrent workload races.  
**Suggested test:** Implement `WT_TIMING_STRESS_LR_SLOW` in `__wti_live_restore_fs_restore_file`. Enable in existing live restore Python tests under timing stress mode.

---

### [WT-14395] — Crash during checkpoint should not advance `oldest_timestamp` (MEDIUM)
**Gap:** Labeled `model-test`. Crash at final checkpoint crash point produces different oldest_timestamp results depending on whether logging is enabled. Model doesn't capture logging-enabled variant.  
**Suggested test:** Extend test/model's `checkpoint_crash` workload to parameterize over `log=(enabled=true/false)`. Assert oldest_timestamp after recovery equals 50 (not 100) in both cases.

---

### [WT-16421] — Checkpoint cursor + bulk cursor error path (MEDIUM)
**Gap:** Error path at `cur_file.c:1091` ("checkpoints are read-only and cannot be bulk-loaded") is unreachable in any existing test.  
**Suggested test:** Open checkpoint cursor, then attempt bulk cursor on same table URI. Assert `EINVAL`.

---

### [WT-16923] — Dirty bytes stat in checkpoint progress messages (LOW)
**Gap:** WT-16912 added dirty bytes per btree in checkpoint progress messages. No test verifies stat decrements correctly during checkpoint.  
**Suggested test:** Write fixed amount of data, trigger checkpoint, parse WT verbose log, assert `dirty_bytes_at_start >= bytes_written_by_checkpoint`.

---

### [WT-17181] — Compatibility testing for minor releases not on par with major (MEDIUM)
**Gap:** Starting with MongoDB 8.2, minor releases are production-quality and require same upgrade/downgrade testing. No test matrix covers adjacent minor release pairs (e.g., 8.2 ↔ 8.1).  
**Resolution:** Audit compatibility test matrix; add version pairs `(8.1, 8.2)`, `(8.2, 8.3)` etc.

---

### [WT-17381] — TSAN data race in `__wt_delete_page_rollback` (MEDIUM)
**Gap:** Real TSAN data race between `__wt_delete_page_rollback` and a concurrent cursor reader on instantiated tombstone. Found via test/format TSAN build. Unconditional write to `upd_saved_txnid` at `bt_delete.c:302` clobbers `upd_start_ts` for non-prepared rollbacks.  
**Suggested test:** Add the existing test/format TSAN reproducer as a named CI config. Once fixed, add TSAN-enabled targeted concurrent test (open cursor + rollback prepared delete page in parallel).

---

### [WT-14688] — Live restore multi-node test coverage (LOW)
**Gap:** Only a single MongoDB server test exercises live restore (single-node). Multi-node interaction (live restore + oplog truncation, live restore + stepdown) untested.

---

## 9. Disabled/Skipped Tests Requiring Attention

| File | FIXME/Guard | Description | Priority |
|---|---|---|---|
| `test/suite/test_corrupt01.py:38-39` | `FIXME-WT-15064: disabled until corruption tests for DisAgg` | ALL WT corruption detection tests skipped under disagg hook | HIGH |
| `test/suite/test_layered31.py:264-284` | `if False:` — `FIXME-WT-14545: enable when stepping down is debugged` | Cursor positioning across step-down entirely untested | HIGH |
| `test/suite/test_prepare34.py:35` | `# FIXME: Verify that prepared modifies are reconstructed properly when loaded from disk` | Prepared modify on-disk reconstruction not validated after crash+recovery | HIGH |
| `test/suite/test_layered43.py:58` | `FIXME-WT-15663: block cache is disabled` | Victim block cache code path untested in CI | HIGH |
| `test/suite/test_layered27.py:91,150,270` | `FIXME-WT-15763: re-enable once step-down works` | 3 step-down scenarios disabled (core disagg requirement) | HIGH |
| `test/suite/test_layered88.py:51` | `FIXME-WT-17177: read-only connection with disagg must be rejected` | Read-only connections not validated to return error in disagg mode | MEDIUM |
| `test/suite/test_stat10.py:108` | `FIXME-WT-16633: re-enable once fixed` | Stat correctness test disabled (may be fixable now) | MEDIUM |
| `test/suite/test_sweep04.py:113-114` | `FIXME-WT-13706`, full hard skip | Both tests skipped in ALL configurations; original bug may be fixed | LOW |
| `test/suite/test_truncate23.py:127` | `FIXME-WT-13232`, full hard skip | Same as above | LOW |

---

## 10. Major New Testing Areas (Big Picture)

The following are the broadest new testing areas identified — themes that span multiple gaps and represent categories of testing entirely absent from the existing 110-gap synthesis.

### Area 1: Publish API End-to-End (WT-17087/88/89/90/91)
A complete new `WT_SESSION::publish()` API covering leader-side publishing, follower-side queue management, checkpoint pickup pruning, step-up table creation from queue, and step-down queue preservation. Zero existing tests because the API doesn't exist yet. This is the largest single new test surface identified.

### Area 2: Precise Checkpoint as the Default (WT-15227)
The Python hook never sets `precise_checkpoint=true`, making most disagg Python tests weaker than production. This is a structural deficiency: fixing it converts every existing test into a stronger test. The acceptance work involves classifying which existing tests can and cannot use precise checkpoints.

### Area 3: Verification Completeness in Disagg Mode (WT-14915, WT-15064, WT-17146, WT-17188, WT-17125)
- HS verification disabled (`FIXME-WT-10779` at `bt_vrfy.c:1267`)
- All corruption tests disabled (`test_corrupt01.py` entirely skipped)
- Shared metadata cross-check not in verify
- btree ID uniqueness in shared metadata not checked
- `read_corrupt=true` mode not exercised in disagg
Together these mean `session.verify()` in disagg mode provides drastically weaker guarantees than in non-disagg. A systematic disagg verify improvement effort is needed.

### Area 4: Checkpoint Pickup Performance Regression Testing (WT-16188, WT-17307, WT-17352)
Checkpoint pickup at 250k tables takes 27+ minutes (violates the 15-minute RTO SLA). No automated regression test detects performance degradation. A parameterized performance test measuring pickup time vs. table count would prevent future regressions.

### Area 5: Follower Write Correctness Under Timestamp Visibility Edge Cases (WT-17247, WT-17278)
Two separate bugs show that the follower write path (`__clayered_remove_follower`, `__clayered_insert`, `__clayered_update`) has incorrect time-window handling for stable cells. One is a Critical P2 with a drain assertion that fires in production. Neither has a targeted regression test. A comprehensive follower-write test matrix covering all (read_ts < stop_ts / read_ts == stop_ts) × (remove / insert / update / modify) scenarios is needed.

### Area 6: Role Transition Stress Testing (WT-15860, WT-16879, WT-14949)
Three distinct race conditions at step-up/step-down: (a) internal service threads executing role-sensitive code at transition time, (b) btree-open TOCTOU race against step-down, (c) API calls received during reconfigure window. No test currently exercises any of these concurrency scenarios.

### Area 7: `wt` Utility Tool in Disagg Mode (WT-15940, WT-17341, WT-17345/17346)
The `wt` command-line tool has zero disagg-specific test coverage. Known open bugs: (a) misleading error when run against non-disagg DB with disagg config; (b) unsupported subcommands (backup, salvage, compact) in disagg mode should return clean errors rather than crashing; (c) new subcommand to read individual pages through `WT_PAGE_LOG` (WT-17341). This is an entire tool-level surface unexercised by any test.

### Area 8: Prepared Fast Truncate End-to-End (WT-15565, WT-17277, WT-14879)
Prepared fast truncate operations are not written to disk with a prepared-id encoding. The `FIXME` at `prepared_discover_walk.c:328` confirms step-up reconstruct doesn't handle them. `test/format` silently disables truncate whenever `prepare + precise_checkpoint` are both on (`format_config.c:1548`). Six related tickets (WT-17274-17277, WT-15565) none with tests. A full test suite (Python functional, crash recovery, format stress) is needed.

### Area 9: GC Validation — Ingest vs. Stable vs. HS Content (WT-15476, WT-17189/90/92)
Active development (target 2026-05-15) that needs tests added alongside the implementation — debug-build mismatch detection, HS cross-check, and release-build probabilistic sampling. The implementation has already surfaced several subtle failure modes (lazy page-fetch race, delete-sentinel misidentification, `preserve_prepared` interactions) that each need targeted tests.

### Area 10: Follower API Surface Coverage via Hook Extension (WT-14541)
The disagg hook intentionally skips almost all tests in follower mode. By extending the close path to flush-to-leader-checkpoint before closing, the hook can enable a large class of existing tests to run in follower role with minimal new code. This would dramatically increase follower coverage.

---

## 11. Uncertain Cases (May or May Not Be Covered)

These cases need code search or targeted investigation to confirm whether they are covered.

| ID | Ticket | Question |
|---|---|---|
| UC-1 | WT-14879 | Fast truncate tests exist but none check whether delta IS or IS NOT generated for truncated pages. Behavior contractually untested. |
| UC-2 | WT-16159 | PALite lacks multi-process support; leader-follower switch with concurrent processes may not be truly tested in CI. |
| UC-3 | WT-16224 | Progressive delta unpack optimization — tests may pass with either old or new implementation, making regressions undetectable. |
| UC-4 | WT-15027 | Percentage-of-modified-rows delta heuristic — not implemented yet; test design pending design decision. |
| UC-5 | WT-16810 | Leader-mode cursor with non-empty ingest — `if (leader)` guards prevent this scenario; will become a gap when async draining is enabled. |
| UC-6 | WT-14895 | `__clayered_lookup` + `__clayered_put` double traversal — concurrency race between lookup and put may be covered implicitly by format stress tests. |
| UC-7 | WT-14521 | GC safety under pinned txn IDs — may be partially covered by existing GC tests. |
| UC-8 | WT-15159 | HS delta reconciliation — performance investigation, may need a stat-based threshold assertion. |
| UC-9 | WT-16982 | Sweep server correctly skips layered/ingest dhandles — no test confirms the skip, but the behavior may be exercised by existing sweep tests. |
| UC-10 | WT-14494 | HS dhandle flag identification in disagg — may need code review to determine if existing GC/MVCC tests cover the shared HS path. |
| UC-11 | WT-14537 | Leader/follower mode stat transition — likely covered by implementation PR, but no confirmed regression test. |
| UC-12 | WT-16260 | Expired history testing — dependencies (3 unnamed tickets + SERVER-115340) make scope unclear. |
| UC-13 | WT-17160 | Cache stuck in test_layered91 — scalability/eviction bug limiting test breadth rather than a gap per se. |
| UC-14 | WT-17127 | `bt_vrfy.c` uses `strcmp(name, WT_METAFILE_URI)` instead of `WT_IS_URI_METADATA` — may miss shared metadata URI in skip_hs logic. |
| UC-15 | WT-16002 | Materialization frontier boundary conditions — unclear if frontier boundary cases are tested. |
| UC-16 | WT-14037 | Eviction enqueuing non-evictable pages — open PR; unclear if fix includes regression test. |
| UC-17 | WT-14031 | `op_timer_fired` not freeing threads stuck in eviction — de-prioritized, no regression test. |

---

## 12. Cross-Reference: New Gaps by Source File

| Finding File | Confirmed Gaps | Uncertain | Priority High-Level |
|---|---|---|---|
| `01_pali_page_delta.md` | 10 | 4 | 1 HIGH, 7 MEDIUM, 2 MEDIUM/INFRA |
| `02_layered_cursor.md` | 12 | 2 | 1 CRITICAL, 5 HIGH, 6 MEDIUM |
| `03_metadata_gc_ingest.md` | 13 | 3 | 7 HIGH, 4 MEDIUM, 2 MEDIUM |
| `04_follower_leader_roles.md` | 17 | 5 | 9 HIGH, 6 MEDIUM, 2 DEFERRED |
| `05_precise_ckpt_prepare_rts_truncate.md` | 17 | 4 | 3 HIGH, 11 MEDIUM, 3 LOW |
| `06_validation_verification_testing.md` | 14 | 3 | 5 HIGH, 7 MEDIUM, 2 LOW |
| `07_stories_epics_remaining_disagg.md` | 17 | 4 | 8 HIGH, 8 MEDIUM, 1 infra |
| `08_non_disagg_and_fixmes.md` | 15 non-disagg + 8 FIXME | 5 | 3 HIGH, 9 MEDIUM, 3 LOW |

---

## 13. Priority-Ranked Quick Reference

### CRITICAL (DisAgg)
| ID | Ticket | Description |
|---|---|---|
| CW-H8 | WT-17247 | Follower writes ignore `stable.stop_ts > read_ts` — drain assertion fires |

### HIGH (DisAgg, new gaps)
| ID | Ticket | Description |
|---|---|---|
| CR-H11 | WT-15189 | `next_random()` infinite spin on all-tombstoned ingest (active CI failure) |
| — | WT-17278 | Follower remove: WT_NOTFOUND where leader expects WT_ROLLBACK |
| — | WT-15064 | Corruption detection tests entirely disabled for disagg |
| — | WT-17087-91 | Publish API cluster: zero test coverage for entire new API |
| — | WT-16879 | Dhandle-open / step-down TOCTOU race |
| — | WT-14949 | Error code when WT API called during reconfigure window |
| — | WT-15808 | Read cursor survival behavior during step-up |
| — | WT-17090 | Checkpoint pickup vs. local-only table reconciliation |
| — | WT-17309 | Step-up with unreset cursor: no clean-error test |
| FT-GC1 | WT-16813 | Truncate list GC at follower checkpoint pickup |
| TT-GC1 | WT-14521 | GC safety under pinned transaction IDs |
| TT-H3 | WT-16257 | Cross-node oldest_timestamp propagation |
| V-GC1 | WT-14913 | ingest↔stable coherence in verify() |
| V-GC2 | WT-15476 | GC-time mismatch detection (debug build) |
| V-SM1 | WT-17146 | Local↔shared metadata consistency in verify() |
| CP-SCALE | WT-17352 | Checkpoint pickup 27+ min at 250k tables (violates 15-min RTO SLA) |
| CW-H9 | WT-15970 | Positioned cursor across step-up while ingest not yet drained |
| CW-H10 | WT-14563 | Bulk cursor on layered table: EINVAL + no test |
| CW-H11 | WT-15411 | remove() with ambiguous `positioned` flag |
| CQ-H1 | WT-14806 | `largest_key()` when largest key tombstoned in ingest |
| CQ-H2 | WT-14806 | `next_random()` on mostly-tombstoned ingest |
| CQ-H3 | WT-14806 | `modify()` value colliding with tombstone sentinel `\x14\x14` |
| CR-H7 | WT-14545 | Mid-scan cursor across step-down (`if False:` guarded) |
| INFRA-1 | WT-15227 | Python hook never enables `precise_checkpoint=true` — STRUCTURAL |
| — | WT-14491 | Table drop coordination across leader + follower |
| — | WT-15357 | Layered checkpoint cursors not supported |
| — | WT-15594 | Timestamp enforcement on layered table writes |
| — | WT-16494 | Checkpoint order monotonicity across role changes |

### HIGH (Non-DisAgg)
| ID | Ticket | Description |
|---|---|---|
| — | WT-17277 | Prepared fast truncate in test_checkpoint and test_format |
| — | WT-16713 | Victim block cache: zero in-WT test coverage |
| — | WT-16834/36 | Table ID uniqueness enforcement |

---

*See `disagg-analysis/findings/` for detailed per-ticket analysis, JIRA quotes, and complete suggested test specifications.*  
*Reference baseline: `test/analysis/05_scenario_analysis/00_synthesis.md` (110+ prior gaps, not repeated here).*
