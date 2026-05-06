# Gap Analysis: Follower/Leader Roles, Step-Up/Step-Down
Generated: 2026-05-06

## Scope

Covers the 43 Jira tickets in `/tmp/agent_g4.txt` that touch leader/follower node roles, step-up (follower→leader), step-down (leader→follower), standby behavior, publish API, and checkpoint coordination between nodes.

Elegant step-down (calling `conn.reconfigure(role="follower")` without server restart) is **DEFERRED** — step-down today is only via server restart. Any gap that requires elegant step-down to be testable is flagged DEFERRED below.

Existing gaps already reported in `test/analysis/05_scenario_analysis/05_checkpoint_roles.md` and `test/analysis/03_gap_analysis/disagg_layered_role_transitions.md` are referenced by their IDs and not repeated in full.

---

## Confirmed Testing Gaps

### [WT-14541] — Extend hook coverage to include followers
**Type/Priority/Status:** Improvement / Major-P3 / Open  
**Jira context:** Hook currently skips many test cases when the connection is a follower because "ingest changes disappear during reopen" — the proposed fix is to have the hook's `WT_CONNECTION.close()` first call `reconfigure(role="leader")` and wait for a checkpoint before closing, making ingest changes durable so more tests pass in follower mode.  
**Gap:** Almost no existing Python tests exercise the full standard API surface (cursors, schema, timestamps, stats) in follower role. The hook's follower path is an intentional skip, leaving a large class of negative-test scenarios untested: what does `session.verify()` return on a follower? What does `session.compact()` return? Does `session.alter()` succeed or fail on a follower? The only test that currently runs as a "pure follower" with API surface coverage is hook-driven and incomplete.  
**Suggested test:** Extend `hook_disagg.py` close path to do a flush-to-leader-checkpoint trick (as the ticket proposes). Then enable a set of existing tests to run in follower mode — starting with read-only API surface (scan, stat query, compact-check) and then schema operations that should return `ENOTSUP` or `EINVAL` on followers.  
**Already in existing analysis?** Partially — SO-H4 covers `verify()` on follower; this ticket is broader. **NEW GAP for full API surface coverage**.

---

### [WT-14545] — Layered cursors work with step-down
**Type/Priority/Status:** Improvement / Major-P3 / Open (assigned D. Anderson)  
**Jira context:** Cursors that are open at the moment of step-down currently cause `test_layered31` Part 6 to be commented out. The ticket notes "test_layered31 depends on the current work, as part of the test has been commented out because it is failing." The dependency (SLS-1449 — make layered cursors work with step-up) is closed, but step-*down* support is still missing.  
**Gap:** There is no test for a layered cursor that is positioned (i.e., `cursor.search()` has returned successfully) at the moment the node transitions from leader to follower. After the transition, the cursor should see the follower checkpoint view, not the in-flight leader write view. The commented-out code in `test_layered31` is the only near-test for this path, and it is disabled.  
**Suggested test:** Re-enable Part 6 of `test_layered31` once the fix lands in WT-14545. Separately add a test that: (1) positions a cursor on the leader at key K, (2) steps down, (3) calls `cursor.next()` and verifies the cursor sees the follower checkpoint without crash or corruption.  
**Already in existing analysis?** CR-H6 in `05_scenario_analysis/` covers `cursor.bound()` + step_up. Step-*down* cursor interaction is **NEW GAP**.

---

### [WT-14949] — Check all transactions/cursors closed during reconfigure step-up/step-down
**Type/Priority/Status:** Task / Major-P3 / Open (assigned J. Chen)  
**Jira context:** The ticket requires adding safety checks to all API calls so they return an error when `WT_CONN_RECONFIGURING_STEP_UP` or the equivalent step-down flag is set. A comment from D. Anderson notes: "In the future, we may allow read transactions and read-only cursors to stay open across step up/down transitions."  
**Gap:** No Python test verifies that an application which calls any WT API (e.g., `cursor.insert()`, `session.begin_transaction()`) *while* a step-up is in progress receives a defined error code (not a crash). The expected behavior once WT-14949 is implemented — returning `EBUSY` or `EINVAL` during reconfigure — is completely untested. Additionally, the "future relaxation" path (read cursors surviving) is entirely untested.  
**Suggested test:** Two-thread Python test: Thread 1 calls `conn.reconfigure(role="leader")`. Thread 2, immediately after Thread 1 starts, calls `cursor.insert()` or `session.begin_transaction()`. Assert Thread 2 receives a non-fatal error code (not a crash), and that after step-up completes, normal operations resume.  
**Already in existing analysis?** Related to Gap 5 (open transactions during promotion) in `03_gap_analysis/`. The error-code-during-reconfigure aspect is **NEW GAP**.

---

### [WT-15808] — Support readers when performing step-up
**Type/Priority/Status:** Task / Major-P3 / Open (backlog)  
**Jira context:** Found via WT-15742. Investigation ticket to identify what code changes are needed to allow read transactions/cursors to remain open during step-up. Last comment (K. Mahar): "I would be surprised if we didn't [support readers]. It would be a very significant behavioral change from ASC."  
**Gap:** The current behavior when a read cursor is open at step-up time is undefined. `test_layered94` (prepared transactions during step-up) tests prepared transactions but not plain read transactions. No test verifies: (a) what error code a read-only cursor open during step-up receives, (b) whether the cursor remains usable after step-up completes, or (c) what data the cursor sees post-transition (follower checkpoint view vs. live view).  
**Suggested test:** Python test: open a read cursor on the follower, position it with `cursor.search_near()`, call `conn.reconfigure(role="leader")` while the cursor is open, then call `cursor.next()` on the still-open cursor. Assert defined behavior: either the cursor receives a specific error code or it continues returning follower-checkpoint data.  
**Already in existing analysis?** Related to existing Gap 5 (open uncommitted txns during step-up) in `03_gap_analysis/`. Read-only cursor survival during step-up is **NEW GAP**.

---

### [WT-15860] — Internal threads during step-up/step-down
**Type/Priority/Status:** Task / Major-P3 / Backlog  
**Jira context:** Example race in the ticket: (1) node is primary, (2) checkpoint cleanup thread finds a table to clean, (3) node is stepping down, (4) checkpoint cleanup initiates writes — which should not happen. The ticket covers checkpoint cleanup, eviction, background compaction, and similar internal threads.  
**Gap:** No test exercises the specific race where an internal service thread (stat log server, sweep server, checkpoint cleanup) is executing role-sensitive code at the exact moment a step-down or step-up transition occurs. Tests use `timing_stress_for_test=[checkpoint_slow]` for some checkpoint races but nothing for internal-thread/role-transition races.  
**Suggested test:** Use `timing_stress_for_test` or a similar injection to slow the checkpoint cleanup or sweep server, then trigger step-down while they are in flight. Assert that no writes are initiated by the internal thread after step-down completes. This is a new test area requiring cooperation from the internal thread implementation (WT-15860 must be resolved first before the test is fully meaningful).  
**Already in existing analysis?** Not covered. **NEW GAP — internal thread / role transition race**.

---

### [WT-15970] — Layered cursors wait for ingest drain during step-up
**Type/Priority/Status:** Bug / Major-P3 / Open  
**Jira context:** When a cursor survives step-up (future path via SERVER-122542), the layered cursor closes the ingest cursor and reopens the stable cursor. But the ingest cursor must not be closed until the drain of the ingest table into the stable btree is complete. The flag `WT_CONN_RECONFIGURING_STEP_UP` should be consulted. An alternative is a per-`WT_LAYERED_TABLE` drain-completion flag. Kaitlin Mahar moved this to "Open" noting it "could potentially cause severe issues."  
**Gap:** No test exercises an open layered cursor that survives a step-up and then performs operations while the drain is still in progress. The current test suite requires all cursors to be reset before step-up (`WT-17309` comment: "we currently require all cursors to be explicitly reset before stepping up"), so the problematic code path (cursor uses ingest after it should have been closed) is never reached.  
**Suggested test:** Once cursor-survives-step-up support is implemented (WT-17309), add a test that: (1) keeps a layered cursor open on a follower with large ingest data, (2) steps up with `drain_threads=1` and slow drain (timing stress), (3) while drain is in progress, calls `cursor.next()` on the still-open cursor, (4) verifies no data from un-drained ingest is returned prematurely.  
**Already in existing analysis?** Not covered. **NEW GAP — ingest drain completion invariant for surviving cursors**.

---

### [WT-16813] — Follower GC checkpoint pickup with fast truncate
**Type/Priority/Status:** Task / Major-P3 / In Progress (K. Chovhan, active PR)  
**Jira context:** Fast truncate on followers uses an in-memory truncate list (ingest → truncate → stable). Without GC on checkpoint pickup, the list grows unboundedly. GC is triggered by checkpoint pickup: prune truncate entries whose stable table already includes them. The ticket description specifies a functional test requirement: verify that obsolete entries are removed while active/visible truncates remain intact.  
**Gap:** No current test verifies that the truncate list is pruned on checkpoint pickup. `test_layered71` tests table drop during checkpoint but not truncate-list GC. `SO-H2` (truncate on leader) is already flagged in existing analysis. The specific follower GC path — truncate entries accumulating across multiple checkpoints and being pruned — has no Python test.  
**Suggested test:** Python test: leader issues 5 truncate operations across 3 checkpoints. Follower advances to checkpoint N, then checkpoint N+1, then checkpoint N+2. After each pickup, verify via stat or explicit inspection that truncate list entries from before the pickup's stable timestamp are removed, and entries for truncates that overlap the current read window remain. Verify data visibility is correct after each pickup.  
**Already in existing analysis?** CP-H1 (follower advance before leader checkpoint) already in existing analysis. The truncate-list GC aspect is **NEW GAP**.

---

### [WT-16879] — Data race between open btree/dhandle and primary step-down
**Type/Priority/Status:** Task / Major-P3 / Open  
**Jira context:** Detailed race documented in the ticket: Thread A checks `leader==true`, starts building a URI without checkpoint suffix (leader path), enters `__wt_btree_open()` with no `WT_DHANDLE_OPEN` flag set yet. Thread B (step-down) sweeps all dhandles with `WT_DHANDLE_OPEN` set — Thread A's dhandle is skipped because it lacks `WT_DHANDLE_OPEN`. Thread B sets `conn->leader=false`. Thread A finishes `__wt_btree_open()`, setting `WT_DHANDLE_OPEN`, leaving a read-write disagg btree open on a follower node.  
**Gap:** No test deliberately races a btree open with a step-down transition. All existing step-down tests (`test_layered62`, `test_layered64`, etc.) close existing cursors/dhandles cleanly before step-down. The race window described in the ticket is a TOCTOU between the leader-check at dhandle-URI-construction time and the step-down sweep.  
**Suggested test:** Multi-threaded Python test (or C csuite test): Thread 1 in a tight loop opens new layered tables. Thread 2 calls `conn.reconfigure(role="follower")`. After step-down completes, verify that no btree is open in read-write mode on the (now) follower. This requires a debug stat or diagnostic dump to inspect dhandle flags.  
**Already in existing analysis?** Not covered. **NEW GAP — dhandle open / step-down race**.

---

### [WT-17040] — Shared metadata creation necessity on followers
**Type/Priority/Status:** Task / Major-P3 / Open  
**Jira context:** On follower startup, `__disagg_metadata_table_init` opens the live dhandle of the shared metadata table even though followers should never use the live dhandle (only checkpoint dhandles). As a short-term fix, the live dhandle is immediately expired. P. Macko's comment: changing this design would require handling all step-up/step-down implications.  
**Gap:** No test verifies the scenario where a follower starts without creating the shared metadata table, then picks up a checkpoint that creates it, then steps up. The current test flow (all tests create shared metadata at connection open time) does not cover a follower that first sees the shared metadata table only via checkpoint pickup.  
**Suggested test:** Start a follower connection without the shared metadata table pre-existing in local metadata. Provide a `checkpoint_meta` from a leader that has multiple tables. Verify that checkpoint pickup correctly creates the shared metadata locally. Then step up and verify the node can write checkpoints.  
**Already in existing analysis?** Related to Gap 6 (follower with no prior checkpoint) in `03_gap_analysis/`. The shared-metadata creation aspect is **NEW GAP**.

---

### [WT-17063] — Shared disk hash table switch in step-down and step-up
**Type/Priority/Status:** New Feature / Major-P3 / Open  
**Jira context:** The shared disk hash table (used for transaction conflict detection) needs to be initialized/destroyed at step-up/step-down. Discussion underway about whether to keep the hash table alive for some time after step-down to aid cache warm-up on the new leader.  
**Gap:** No test exercises the hash table lifecycle at step transitions. A follower that steps up should have the hash table initialized; a leader that steps down (via server restart currently) should have it destroyed. If the hash table is kept alive for some period post-step-down, no test verifies that stale hash table state does not affect the new leader's conflict detection.  
**Suggested test:** After step-up, insert conflicting key-value pairs from two transactions and verify `WT_ROLLBACK` is correctly returned (hash table is correctly initialized). After step-down (server restart), verify that the new leader starts with a clean conflict table (no phantom conflicts from the old leader's hash table state).  
**Already in existing analysis?** Not covered. **NEW GAP — hash table lifecycle at role transitions**.

---

### [WT-17087] — Publish API for leaders
**Type/Priority/Status:** Task / Major-P3 / Open (assigned P. Macko)  
**Jira context:** New `WT_SESSION::publish(session, uri, epoch)` API. On the leader side: enqueue metadata operations with a `schema_epoch`, skip publishing to shared metadata until `session->publish()` is called, add a check in `__checkpoint_prepare` to capture the stable schema epoch. The publish API gating is: "if `stable_disaggregated_schema_epoch` has not been set, publish automatically without epochs."  
**Gap:** The publish API does not exist yet (Open ticket). When it does, the following test scenarios are missing: (a) leader creates a table, inserts data, calls checkpoint WITHOUT calling `publish` — verifies data is NOT visible to followers (table not in shared metadata), (b) leader calls `publish` with a schema epoch, then checkpoints — verifies table IS visible to followers, (c) leader calls `publish` with an epoch that is LESS than the last published epoch — verifies a clear error, (d) leader creates a table and writes data but NEVER publishes, then steps down (server restart) — verifies the table does not appear in the next leader's view.  
**Suggested test:** New `test_layered_publish_leader.py` covering happy-path publish→checkpoint→follower-pickup, error-path publish-with-wrong-epoch, and checkpoint-without-publish ensuring table is invisible.  
**Already in existing analysis?** Not covered (API does not exist yet). **NEW GAP — publish API leader-side coverage**.

---

### [WT-17088] — Assert no writes to unpublished table
**Type/Priority/Status:** Task / Major-P3 / Open (assigned P. Macko)  
**Jira context:** When `__disagg_shared_metadata_op` is called with `WT_SHARED_METADATA_UPDATE` (i.e., a write is going to the shared metadata for a table), assert that the table already exists in the shared metadata. If it doesn't, panic. This guards against stable writes to unpublished tables.  
**Gap:** No test verifies that writing to an unpublished table triggers a panic (or a clean error). The assertion is being added as an implementation guard (WT-17088), but the test to confirm it fires correctly does not exist. Without the test, if the assert is removed or weakened, there is no regression protection.  
**Suggested test:** Python test (or C-level test) that: creates a table, immediately calls `cursor.insert()` to write data, calls `session.checkpoint()` WITHOUT calling `publish` (when publish is required), and verifies either a panic or a `WT_ERROR` with a message matching "unpublished table." Run this only in `diagnostic` build mode where asserts are active.  
**Already in existing analysis?** Not covered. **NEW GAP — unpublished table write assertion**.

---

### [WT-17089] — Publish API for followers
**Type/Priority/Status:** Task / Major-P3 / Open (assigned P. Macko)  
**Jira context:** On the follower side, the metadata operation queue (`conn->disaggregated_storage.shared_metadata_qh`) is currently assumed to be empty. WT-17089 enables the queue on followers so it can replay pending operations during step-up. When picking up a checkpoint, prune queue entries whose `schema_epoch` is included in `checkpoint_schema_epoch`. Modify `__layered_create_missing_stable_tables` to create missing stable tables from the queue.  
**Gap:** No test exercises the sequence: (1) follower receives a `session->publish()` call for a new table, (2) follower picks up a checkpoint that includes that schema epoch, (3) follower steps up — the queue is consulted to create the missing stable table. This is the critical handoff between the publish queue and step-up.  
**Suggested test:** New `test_layered_publish_follower.py`: leader creates and publishes table T at epoch E, checkpoints. Follower picks up checkpoint and is given the metadata. Follower's internal queue should have the table T entry pruned (table included in checkpoint). Follower steps up. Verify stable table T is created and writable on the new leader without re-issuing any schema operations.  
**Already in existing analysis?** Not covered. **NEW GAP — publish API follower-side queue and step-up interaction**.

---

### [WT-17090] — Reconcile checkpoint pickup with metadata operations on follower
**Type/Priority/Status:** Task / Major-P3 / Open (assigned P. Macko)  
**Jira context:** Two reconciliation cases: (a) table exists in checkpoint's shared metadata but not locally — the follower's queue should have a published drop ahead of the checkpoint epoch, or WiredTiger should pick up the table; (b) table exists locally but not in checkpoint's shared metadata — the follower's queue should have an unpublished create, or WiredTiger should drop it locally. Dropping a table during checkpoint pickup may return `EBUSY`.  
**Gap:** No test exercises either reconciliation case. Specifically: scenario (b) — follower creates a table locally (as a new-table candidate for publish), then picks up a leader checkpoint that does NOT include that table (because it was never published). The follower should detect this divergence and handle it (either drop the local table or queue it for deferred creation). No test verifies this behavior.  
**Suggested test:** Python test: follower opens a connection and is given a checkpoint from a leader. Follower calls `session.create('layered:t2', ...)` locally (as if it is preparing to publish). Then follower picks up a second checkpoint from the leader that does NOT include t2 (because it was never published on the leader). Verify: (a) no crash, (b) t2 is either dropped locally or inaccessible on the follower, (c) the follower can pick up subsequent checkpoints normally.  
**Already in existing analysis?** Not covered. **NEW GAP — checkpoint pickup vs. local-only table reconciliation**.

---

### [WT-17091] — Step-down for publish
**Type/Priority/Status:** Task / Major-P3 / Open (assigned P. Macko)  
**Jira context:** Step-down currently clears the metadata operation queue (`__disagg_step_down`). This is wrong for the publish flow: the queue must be preserved so that a subsequent step-up can replay the pending operations. The ticket notes this may interact with the broader step-down design and is flagged for coordination.  
**Gap:** No test verifies that the metadata operation queue is preserved across a step-down. Since elegant step-down is via server restart only, this means: after a server restart (which is step-down), the queue state is in memory only and is lost — this is actually the correct behavior for now. The gap opens up when elegant step-down is implemented. For now, the gap is: no test verifies that a leader which has pending publish operations (not yet in a checkpoint), does a server restart (step-down), and then a new leader starts — the new leader does NOT see those unpublished operations in its queue.  
**Suggested test (current):** Leader creates table T, calls `publish(T, epoch=5)`, but does NOT call `session.checkpoint()`. Simulate server restart (step-down). Start a new leader with `checkpoint_meta` from the last complete checkpoint (which does not include T). Verify T is NOT visible to followers.  
**Already in existing analysis?** Not covered. **NEW GAP — step-down clears publish queue correctly**.

---

### [WT-17309] — Step-up without resetting all cursors
**Type/Priority/Status:** Task / Major-P3 / Backlog  
**Jira context:** Currently all cursors must be reset before step-up. The ticket was created by I. Kochin. A comment from I. Kochin (2026-05-01): "We implemented an initial fix for the current requirements in WT-17331 to always reopen the stable table on a role change. This should be fine since we currently require all cursors to be explicitly reset before stepping up. However, we should still relax the step-up requirements for async step-up/elegant step-down."  
**Gap:** No test verifies that a step-up fails with a defined error if a cursor is NOT reset. The current code panics or asserts if a cursor is open during step-up; no test deliberately triggers this to confirm the error is clean (not a silent data corruption). Additionally, no test exercises the relaxed path (cursor survives step-up) which is needed for WT-15808 and WT-15970.  
**Suggested test:** (a) Negative test: open a cursor, do NOT reset it, call `conn.reconfigure(role="leader")`, verify the call returns a defined error (not a crash or assert). (b) Once relaxation is implemented (WT-15808), positive test: open a read cursor, call step-up, verify cursor continues to function and returns the new leader's data view.  
**Already in existing analysis?** CR-H6 (`cursor.bound()` + step_up) is in existing analysis. The explicit error-on-unreset-cursor test is **NEW GAP**.

---

### [WT-17307] — Large table count causes standby lag
**Type/Priority/Status:** Task / Major-P3 / Open  
**Jira context:** Creating 50k+ tables (via 25k+ collections in mongod) causes standby to lag behind primary by 2x or more. Root cause under investigation as part of WT-17352 (checkpoint pickup performance epic). Last comment: "The technical design is in-review."  
**Gap:** No Python test benchmarks or exercises the checkpoint pickup latency with large numbers of tables. `test_layered29` creates 10,000 tables but without data and without measuring pickup latency. There is no regression test that would detect a 2x slowdown in checkpoint pickup for large table counts.  
**Suggested test:** Python perf test: create 5,000 layered tables with 100 rows each on the leader, checkpoint, then measure follower pickup latency. Assert that pickup completes within a timeout proportional to table count (e.g., less than N milliseconds per table). This serves as a regression detector for the standby lag issue.  
**Already in existing analysis?** Gap 11 (large table count with sparse IDs) in `03_gap_analysis/` covers a related area. The lag/latency regression aspect is **NEW GAP**.

---

## Uncertain Cases

### [WT-14537] — WT stat to indicate leader/follower mode
**Type/Priority/Status:** Improvement / Major-P3 / Open  
**Jira context:** Add a stat to indicate whether WT is in leader or follower mode.  
**Uncertain:** No test verifies that the stat transitions correctly at step-up. This is likely to be covered as part of WT-14537's implementation (the impl PR will presumably add a test). Flagged here as potentially missing a regression test, but cannot confirm without seeing the PR.  
**Suggested test:** After step-up, read the "leader mode" stat and assert it is `1`. After step-down (restart), read it on the follower and assert it is `0`.

---

### [WT-15453] — Dedicated API for adopting checkpoints on standby
**Type/Priority/Status:** New Feature / Major-P3 / Backlog  
**Jira context:** Proposes a dedicated API for standby checkpoint adoption (currently done via `conn.reconfigure(checkpoint_meta=...)`).  
**Uncertain:** Since this is a future API not yet designed, there is nothing testable. Flagged as: once the API lands, all existing `disagg_advance_checkpoint` tests should be re-validated against the new API.

---

### [WT-15763] — Graceful step-down support
**Type/Priority/Status:** Task / Major-P3 / Backlog  
**Jira context:** Investigation ticket for graceful (elegant) step-down. Currently DEFERRED.  
**Uncertain:** This is explicitly deferred. No testing is possible until the implementation is in place. All step-down-specific gaps (SD-1, SD-2, SD-3 from existing analysis) depend on this.

---

### [WT-16477] — Read shared metadata directly when opening dhandle on shared table on standby
**Type/Priority/Status:** Improvement / Major-P3 / Open  
**Jira context:** Avoid taking the checkpoint lock when opening a shared table dhandle on standby by reading shared metadata directly.  
**Uncertain:** This is a performance optimization. No functional gap per se, but a regression test to verify the checkpoint lock is NOT acquired (via a stat counter or lock contention test) would confirm the optimization is working. Not clear whether a test is planned as part of the implementation.

---

### [WT-16544] — Slow checkpoint pick-up investigation
**Type/Priority/Status:** Task / Major-P3 / Open  
**Jira context:** Investigation ticket. Likely related to WT-17307 (large table count standby lag) and WT-17352 (performance epic).  
**Uncertain:** Investigation ticket; no test gap directly identified until root cause is determined.

---

### [WT-17093] — Redefine rules for fake checkpoint order
**Type/Priority/Status:** Task / Major-P3 / Open  
**Jira context:** Redefines ordering rules for fake checkpoints (checkpoints that do not advance the page-log LSN). Relevant to follower checkpoint pickup ordering.  
**Uncertain:** Implementation is not yet complete. Once rules are defined, the existing idempotent checkpoint test (`test_layered53`) should be extended to cover the new ordering rules.

---

### [WT-17312] — RandomCursor hang on standby from analyzeShardKey command
**Type/Priority/Status:** Bug / Major-P3 / Open  
**Jira context:** Production bug — `analyzeShardKey` issues a random cursor on a standby, causing a hang.  
**Uncertain:** This is a production-observed hang, not a missing test scenario per se. A regression test should be added once the fix lands. Test: open a random cursor on a follower, call `cursor.next()` several times, verify no hang (use a timeout).  
**Suggested test (once fixed):** `test_layered_random_cursor_follower` — opens a `next_random` cursor on a follower, reads 100 records, verifies completion within a timeout.

---

### [WT-14779] — Segfault `__wt_evict_file` during shutdown on standby
**Type/Priority/Status:** Bug / Major-P3 / Open (tagged `lc_bulk_04_29_26`)  
**Jira context:** `hello_with_standby.js` triggers a segfault in `__wt_evict_file` during standby shutdown.  
**Uncertain:** This is an active bug, not a missing test. Once fixed, a regression test should verify that connection close on a follower with active eviction completes without segfault. Related to existing Gap 14/15 in `05_scenario_analysis/05_checkpoint_roles.md`.

---

## No Gap / Notes

### [WT-15189] — Python tests time out in `clayered_next_random`
Already manifesting as a build failure. This is a test infrastructure issue (timeout), not a missing test scenario. Tracked separately.

### [WT-15788] — test/format disagg multi: send checkpoint metadata leader→follower
This is a test infrastructure improvement (format tests, not Python suite). No Python test gap; the C format test infrastructure gap is tracked in the ticket itself.

### [WT-16113] — Consolidate disagg leader data validation
Improvement to test organization, not a missing scenario. Covered by existing leader validation format tests.

### [WT-16238] — Build failure on ubuntu2004-arm64
Build failure ticket, not a test scenario gap.

### [WT-17008] — 3.92% regression in disagg_step_up_time
Performance regression (build failure/tracking). Not a functional test gap.

### [WT-15970] detail — cursors wait for drain
Already covered in the confirmed gap above (WT-15970). The SERVER-122542 dependency (cursors survive step-up in MongoDB) is the prerequisite; until that lands, the Python test is untestable.

### [WT-17131] — Follower cursors should not reopen unchanged stable table
Performance optimization. No functional test gap, but a regression test that measures checkpoint-pickup cursor-reopen count would be useful.

### [WT-17135] — Enable fast truncate on develop (follower mode)
Implementation ticket. Testing should be added as part of WT-16813 test suite when fast truncate is enabled.

### [WT-17247] — Layered cursor writes on follower do not check stable cell's full time window
Critical P2 bug. Fix is tracked; regression test should be added as part of the fix PR.

### [WT-17278] — Follower remove returns WT_NOTFOUND where leader returns WT_ROLLBACK
P3 bug with a data mismatch implication. Fix and regression test tracked in the ticket.

### [WT-17338] — Auto-pick up latest checkpoint in disagg follower mode for wt tool
Sub-task for the `wt` command-line tool. Not a Python test suite gap.

### [WT-17349] — Support reading individual pages in follower mode without checkpoint pickup
Sub-task. No Python test gap until implemented.

### [WT-17352] — Checkpoint Pickup Performance Epic
Epic aggregating WT-17307 and related tickets. No direct test gap beyond what is listed above.

### [WT-16188] — Checkpoint pick-up scales to millions of tables
Related to WT-17307. Test gap already captured in the WT-17307 entry above.

### [WT-16810] — Clarify layered cursor invariants under leader promotion
Three FIXMEs in `src/cursor/cur_layered.c` (lines 697, 958, 1678) all reference this ticket. The FIXME comment is "In leader mode, skip searching ingest as it should be empty." This is an optimization + correctness assertion. Until WT-16810 is resolved, no test should assert that the ingest table is NOT searched on a leader; once resolved, a test should verify the FIXME behavior.

### [WT-17050/17049] — Perf optimizations (avoid reopening stable table, avoid search for existing keys)
Performance optimizations. No functional test gap; existing correctness tests cover the paths being optimized.

### [WT-17061] — Set close idle time for sweep server on follower
Behavior change for resource management. Regression test needed once implemented: verify that idle ingest table dhandles on a follower are swept after the configured idle time.

---

## New Testing Areas (Not Covered by Any Existing Analysis)

The following represent entirely new test areas identified from this analysis, not previously captured in any scenario or gap document:

1. **Publish API end-to-end** (WT-17087, WT-17088, WT-17089, WT-17090, WT-17091): A complete test suite for `session->publish()` covering leader-side publishing, follower-side queue management, checkpoint pickup pruning, step-up table creation from queue, and step-down queue preservation. Zero existing tests because the API is not yet implemented.

2. **Internal thread / role transition race** (WT-15860): A stress test that runs service threads (checkpoint cleanup, stat log, sweep) while simultaneously triggering step-up or step-down. No existing test infrastructure for this.

3. **Dhandle-open / step-down TOCTOU race** (WT-16879): A multi-threaded test that races btree opens against a step-down transition. Requires debug tooling (dhandle flag inspection) not currently in the Python test suite.

4. **Hash table lifecycle at role transitions** (WT-17063): Tests for the shared disk hash table initialization at step-up and destruction at step-down. Completely absent.

5. **Checkpoint pickup latency regression** (WT-17307, WT-17352): A performance regression test for checkpoint pickup with large table counts. `test_layered29` creates tables but has no latency assertion.

6. **Follower API surface coverage via extended hook** (WT-14541): Running the existing standard WT API test suite in follower mode by extending `hook_disagg.py` to flush changes before connection close. This would turn many existing tests into de facto follower-mode tests with minimal new code.

7. **Cursor reopen at step-up without full cursor reset** (WT-17309, WT-15970): The path where a cursor survives step-up and waits for ingest drain is a fundamentally new code path (currently always panics or is blocked by the "reset all cursors" requirement). An entirely new sub-suite of cursor-survives-transition tests is needed once WT-17309 is implemented.

---

## Summary Table

| Priority | Ticket | Gap Description | NEW/Existing |
|---|---|---|---|
| HIGH | WT-14949 | Error code when API called during reconfigure step-up | NEW |
| HIGH | WT-15808 | Read cursor survival and behavior during step-up | NEW |
| HIGH | WT-16879 | Dhandle-open / step-down TOCTOU race — no test | NEW |
| HIGH | WT-17087 | Publish API leader-side: no tests (API not yet implemented) | NEW |
| HIGH | WT-17089 | Publish API follower queue + step-up interaction: no tests | NEW |
| HIGH | WT-17090 | Checkpoint pickup vs. local-only table reconciliation | NEW |
| HIGH | WT-17309 | Step-up with unreset cursor: no clean-error test | NEW GAP extends CR-H6 |
| HIGH | WT-16813 | Truncate list GC on follower checkpoint pickup | NEW |
| MEDIUM | WT-14541 | Full API surface in follower mode via extended hook | NEW |
| MEDIUM | WT-14545 | Layered cursor positioned during step-down | NEW (CR-H6 covers step-up) |
| MEDIUM | WT-15860 | Internal thread / role transition race | NEW |
| MEDIUM | WT-15970 | Cursor waits for ingest drain completion during step-up | NEW |
| MEDIUM | WT-17063 | Hash table lifecycle at step transitions | NEW |
| MEDIUM | WT-17088 | Assertion: write to unpublished table triggers panic | NEW |
| MEDIUM | WT-17091 | Step-down clears publish queue correctly | NEW |
| MEDIUM | WT-17307 | Checkpoint pickup latency regression with large table counts | NEW |
| LOW | WT-17040 | Follower starts without pre-existing shared metadata | NEW extends Gap 6 |
| DEFERRED | WT-15763 | Elegant step-down: all SD-* gaps depend on this | DEFERRED |
