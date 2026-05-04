# Checkpoint Operations and Role Transitions: Scenario Gap Analysis

## Coverage Summary

| Operation | Coverage Level | Dedicated Tests |
|---|---|---|
| `session.checkpoint()` — basic leader checkpoint | Good | test_layered01–04, test_layered23, test_layered45, test_layered53 |
| `session.checkpoint()` — with prepared transactions in flight | Partial | test_layered45 (single boundary), test_layered94 |
| `session.checkpoint(force=true)` | Minimal (one test, not disagg-specific) | test_layered63 (timestamp use only) |
| `session.checkpoint('name=...')` — named checkpoints | None (hook skips all) | — |
| Follower `session.checkpoint()` — rejected / no-op | Partial | test_layered53 |
| `conn.reconfigure(role="leader")` — step_up | Good (happy path) | test_layered15, 26, 27, 31, 36, 46, 60, 62, 68, 94 |
| `conn.reconfigure(role="follower")` — step_down | Good (happy path) | test_layered15, 26, 62, 64 |
| leader → follower → leader (same connection, no intermediate pickup) | **None** | — |
| leader → follower → leader (connection restart between) | Good | test_layered15, 36, 46, 68 |
| follower → leader → follower → leader multi-hop | None | — |
| Rapid / back-to-back step_down + step_up | None | — |
| step_up with in-flight prepared transactions | Good | test_layered94 |
| step_up when ingest has only uncommitted data | None | — |
| step_down concurrent with checkpoint | Good | test_layered62 |
| step_up concurrent with checkpoint | Good | test_layered62 |
| `disagg_advance_checkpoint` — follower picks up leader checkpoint | Good | 30+ tests |
| `disagg_advance_checkpoint` — follower has open reader snapshot | Good | test_layered72 |
| `disagg_advance_checkpoint` — before leader has checkpointed | None | — |
| `disagg_advance_checkpoint` — idempotent same checkpoint | Good | test_layered53 |
| cursor open → checkpoint → cursor continues (follower) | Good | test_layered31, test_layered85 |
| cursor positioned mid-scan when checkpoint advances (follower) | Good | test_layered31, test_layered85 |
| backup cursor (checkpoint=X) on layered tables | None (hook skips all) | — |
| Connection close during step_up / drain | None | — |
| Two connections both calling step_up simultaneously | None | — |
| Named checkpoint negative test (explicit error check) | None | — |

---

## Gap Analysis

### session.checkpoint()

**Covered:**
- Basic checkpoint on leader: tested in virtually every layered test that writes data; `test_layered01`–`test_layered04`, `test_layered23`, `test_layered53`, `test_layered_modify01`.
- Empty checkpoint advancing only stable timestamp: `test_layered53` verifies that a checkpoint with no new data still writes a page-log record with the updated stable timestamp.
- Checkpoint with a prepared transaction outstanding (no commit yet): `test_layered45` (`test_prepare_update`, `test_prepare_delete`, `test_prepare_update_delete`) verifies that checkpoint skips the page that owns the pending prepared update and that delta statistics are correct.
- Prepared transaction in-checkpoint before step-up: `test_layered94` parametrises over `in_checkpoint=True/False` across insert/update/delete variants.
- Checkpoint concurrent with new table creation: `test_layered60` uses `timing_stress_for_test=[checkpoint_slow]` and creates an empty table mid-checkpoint.
- Checkpoint concurrent with table drop: `test_layered71` drops an empty table while a checkpoint is running.
- Step_down concurrent with an in-progress checkpoint: `test_layered62` (Part 2) starts a checkpoint, waits for it to start, then calls step_down and verifies the checkpoint completed with the leader role.

---

**Gap 1 [HIGH]: Checkpoint immediately after step_up on a connection that has just picked up a follower checkpoint**

- Scenario: Follower receives a page-log checkpoint from the leader (via `disagg_advance_checkpoint`). Without writing any new data, the follower calls `reconfigure(role="leader")` and immediately calls `session.checkpoint()`. The resulting checkpoint should contain exactly the stable state from the picked-up checkpoint plus the drain of any follower-side ingest data (if present).
- Risk: If the step_up triggers `__disagg_restart_checkpoint` which abandons the leader's existing in-progress checkpoint and then immediately calls checkpoint again, there may be a window where the new checkpoint is started before the ingest drain completes. `test_layered62` (Part 1) does verify that a step_up does NOT produce a new disagg checkpoint (checking timestamp == 1 pre-checkpoint), but it does not cover the case where the connection has already picked up a follower checkpoint and immediately checkpoints after step_up.
- Suggested test: `test_layered_stepup_immediate_checkpoint` — follower picks up checkpoint, steps up, immediately calls `session.checkpoint()` without writing any data, verifies checkpoint completes and the follower can pick it up correctly.

---

**Gap 2 [HIGH]: `session.checkpoint('force=true')` on a disaggregated leader**

- Scenario: Set stable timestamp to T, write data at T+10 (above stable), and call `session.checkpoint('force=true')`. Verify the checkpoint does NOT include data above stable, that the checkpoint timestamp is T, and that the page log contains a complete checkpoint record.
- Risk: `force=true` in standard WiredTiger skips the "no dirty data" optimisation and forces a full reconciliation pass. On a disagg leader, this interacts with the `precise_checkpoint` flag and the abandon-checkpoint logic in `__disagg_restart_checkpoint`. `test_layered63` is the only layered test calling `session.checkpoint("use_timestamp=true,force=true")` (line 396) — it is incidental to an internal-page-delta test and does not explicitly verify force semantics in the disagg context.
- Suggested test: Add a sub-case to `test_layered17` or `test_layered53` that uses `force=true` and asserts that the resulting checkpoint matches the stable timestamp, not the commit timestamp of the uncommitted data.

---

**Gap 3 [MEDIUM]: Checkpoint on follower explicitly called by application — should be rejected or be a no-op**

- Scenario: A connection in follower role calls `session.checkpoint()` explicitly. `test_layered53` verifies that such a checkpoint does NOT advance the global page-log checkpoint (checking that `last_checkpoint` remains at 20 after a follower checkpoint), but it does not assert the return value, check for an explicit error, or verify that no page-log write was attempted.
- Risk: If `session.checkpoint()` on a follower silently no-ops without any error, application code that expects the checkpoint to persist data could silently continue with stale data. The negative-assertion in `test_layered53` is partial — it only checks the metadata LSN, not whether the follower checkpoint call failed or was a no-op.
- Suggested test: Extend `test_layered53` to assert that `session.checkpoint()` on a follower either (a) returns a specific error code, or (b) is a documented no-op and does not produce any page-log record, and that the page-log write-count statistic did not increment.

---

**Gap 4 [DEFERRED]: Named checkpoints — no negative test confirming correct error**
*(Named checkpoints are not supported in disagg; negative test tracked as NC-1 in 08_unsupported_features.md.)*

- Scenario: Call `session.checkpoint('name=my_ckpt')` on a disaggregated connection. The hook (`hook_disagg.py` line 249–250) skips any test that calls this, so no test ever reaches the actual implementation path. There is no test confirming the precise error code or message that the implementation returns when a named checkpoint is attempted.
- Risk: If the skip is ever removed or the hook logic changes, a named checkpoint could silently succeed in a way that is not safe for the two-btree architecture (e.g., it could write a named checkpoint to only the ingest btree and not the stable btree, producing a partially-named checkpoint). The absence of a negative test means this is an invisible gap.
- Suggested test: Add a small direct Python test (not through the hook) that opens a disagg connection and calls `session.checkpoint('name=my_ckpt')`, asserting it returns `EINVAL` or `WT_ERROR` with a message matching `"named checkpoint"` or similar.

---

**Gap 5 [LOW]: `force_stop` checkpoint on a disaggregated leader**

- Scenario: Call `session.checkpoint('force_stop=true')` on a disagg leader. Verify that the in-progress checkpoint is cleanly halted and that no partial checkpoint record is written to the page log.
- Risk: `force_stop` in standard WiredTiger aborts an in-progress checkpoint. On a disagg leader, this interacts with `__disagg_begin_checkpoint` and the page-log checkpoint epoch. If `force_stop` is called while the page-log write is in progress, the checkpoint epoch may be left open (neither abandoned nor completed), causing the next checkpoint to fail.
- Suggested test: New test using `timing_stress_for_test=[checkpoint_slow]` to start a slow checkpoint, then call `session.checkpoint('force_stop=true')` from another thread, and verify that the connection is in a clean state afterwards.

---

### Role Transitions (step_down / step_up)

**Note (May 2026):** Elegant step-down (calling `conn.reconfigure(role="follower")` without server restart) is currently only supported via server restart. Tests requiring explicit step-down in Python are untestable until elegant step-down lands in Public Preview. Gaps requiring step-down are marked DEFERRED.

**Covered:**
- follower → leader (step_up): `test_layered15`, `test_layered26`, `test_layered46`, `test_layered60`, `test_layered62`, `test_layered68` — all happy-path step_ups from follower with prior checkpoint pickup.
- leader → follower (step_down): `test_layered15`, `test_layered26`, `test_layered62`, `test_layered64`, `test_layered77` — step_down with eviction stress, concurrent checkpoint.
- follower → leader with prepared transactions in flight: `test_layered94` — comprehensive cover of prepared insert/update/delete, both in-checkpoint and not-in-checkpoint scenarios, commit and rollback.
- Cursor isolation across step_up: `test_layered31` (Part 5) verifies that a cursor opened on the follower continues to read the old checkpoint snapshot after step_up.
- step_up concurrent with an in-progress checkpoint: `test_layered62` (Part 1) checks that a step_up request while a checkpoint has already been started does not corrupt the checkpoint.
- step_down concurrent with a running checkpoint: `test_layered62` (Part 2).
- step_down with eviction of pages split during prior checkpoint: `test_layered77`.

---

*(SD-1, SD-2, SD-3 — step_down → step_up without pickup, rapid step_down + step_up, and multi-hop transitions — are DEFERRED; see `08_unsupported_features.md` for details.)*

**Gap 6 [MEDIUM]: step_up when follower ingest table has only uncommitted data**

- Scenario: On a follower connection, begin a transaction, write several keys, but do NOT commit. Call `conn.reconfigure('disaggregated=(role="leader")')`. The ingest table now has a live uncommitted transaction. Verify that step_up correctly handles the drain — specifically that uncommitted data is not incorrectly copied to the stable btree, and that the uncommitted transaction remains open and can still be committed or rolled back after step_up.
- Risk: `__layered_copy_ingest_table` at `conn_layered_ingest.c:404` uses the condition `prepare || durable_start_ts > last_checkpoint_timestamp`. An ordinary uncommitted (non-prepared) transaction has no `durable_start_ts` — the version cursor would see it as a non-durable update. If the drain incorrectly copies uncommitted updates, they appear in the stable btree without being protected by transactions, and if rolled back later the stable btree still has the phantom data.
- Suggested test: Open follower, begin transaction, write five keys, call step_up without committing, verify that the step_up either (a) rolls back the transaction automatically, or (b) leaves it open and allows commit/rollback, and that the final checkpoint contains exactly the committed data.

---

**Gap 7 [MEDIUM]: step_up failure path — `__wt_panic` is triggered but never tested**

- Scenario: Inject a fault into the page-log layer during `__disagg_begin_checkpoint` so that `__disagg_restart_checkpoint` returns a non-zero error code. Call `conn.reconfigure('disaggregated=(role="leader")')` and observe the behavior.
- Risk: On any failure inside `__disagg_step_up`, the code at `conn_layered.c:1591–1592` calls `__wt_panic` unconditionally. This means a transient I/O error during step_up permanently destroys the connection — there is no retry path. No test verifies: (a) that `WT_PANIC` is returned to the caller, (b) that subsequent operations return `WT_PANIC`, or (c) that a cold restart after a panicked step_up recovers the database to the pre-step_up state.
- Suggested test: A C-level csuite test using a fault-injectable page-log backend, or a Python test with a palite extension modified to fail at `pl_begin_checkpoint`. Alternatively, add debug-mode fault injection via `debug_mode=(xxx)` once such a hook exists.

---

**Gap 8 [LOW]: Explicit step_up while old leader connection is still open (two simultaneous leaders)**

- Scenario: Connection A is the leader. Without closing A, open connection B and call `reconfigure(role="leader")` on B. Verify that the system produces a clear error, or that the page log prevents two concurrent leaders.
- Risk: There is no application-level guard in WiredTiger against two connections on different database home directories both being configured as leader and pointing to the same page log. The page log itself may provide MVCC or write exclusion, but no test verifies the error path or the guarantees. `test_layered31` closes the old leader at line 249 before calling `conn_follow.reconfigure('disaggregated=(role="leader")')` explicitly because of "no confusion within this test" — which acknowledges the risk but does not test the conflict.
- Suggested test: Open two connections (different home directories, same shared page-log kv_home), promote both to leader, verify that one of the step_ups fails with a clear error or that the page log enforces exclusivity.

---

### Follower Checkpoint Advancement

**Covered:**
- Basic `disagg_advance_checkpoint`: used in 38 tests; the primary mechanism for propagating leader checkpoints to followers.
- Idempotent re-advance (same checkpoint twice): `test_layered53` verifies the "Picking up the same checkpoint again" log message and that metadata LSN does not change.
- Follower picks up checkpoint with open read-snapshot: `test_layered72` keeps a read-timestamp transaction open across two consecutive checkpoint advances and verifies history-store data is still accessible.
- Cursor mid-iteration when checkpoint advances: `test_layered31` (Part 4) and `test_layered85` (`test_multiple_checkpoint_advances_during_scan_on_follower`).
- Corrupt checkpoint_meta checksum: `test_layered64` verifies `WT_NOTFOUND` or error when a malformed metadata string is passed.
- Non-existent checkpoint: `test_layered31` (Part 7) passes a fake metadata string and checks `WT_NOTFOUND`.

---

**Gap 9 [HIGH]: Follower calls `disagg_advance_checkpoint` before the leader has produced any checkpoint**

- Scenario: Open a disagg leader, create a table, write data, but do NOT call `session.checkpoint()`. Open a follower. Immediately call `disagg_advance_checkpoint(conn_follow)`. There is no complete checkpoint record in the page log at this point. Verify the behavior: does the call fail with `WT_NOTFOUND`, block, or silently succeed with an empty state?
- Risk: `disagg_get_complete_checkpoint_meta` in `helper_disagg.py` calls `disagg_get_complete_checkpoint_ext` which reads the page-log metadata to find the latest complete checkpoint. If no checkpoint exists, this function's behavior is untested. In the production system this corresponds to a follower coming online before the first leader checkpoint, which is a valid startup scenario that must be handled cleanly.
- Suggested test: New `test_layered_follower_first_advance` — open leader, write data (no checkpoint), open follower, attempt `disagg_advance_checkpoint`, assert either a clean error or that the follower state is empty, then have the leader checkpoint and verify the follower can advance successfully on the second attempt.

---

**Gap 10 [MEDIUM]: Follower advances checkpoint while another follower reader has an old open snapshot**

- Scenario: Two readers open transactions on the follower at timestamp T1. One reader completes and the follower advances its checkpoint to a new one from the leader (with stable timestamp T2 > T1). The second reader continues reading at T1. `test_layered72` tests a single reader with a single checkpoint advance; it does not test two concurrent readers or multiple sequential advances while one reader remains open.
- Risk: The history-store dhandle pinning mechanism (stable btree HS) may leak pinned dhandles if a checkpoint advance happens while more than one reader session holds history-store pins.
- Suggested test: Extend `test_layered72` with two concurrent reader transactions at different timestamps and three checkpoint advances to verify that HS dhandle pins are correctly managed across multiple pickups.

---

**Gap 11 [MEDIUM]: Concurrent `disagg_advance_checkpoint` calls from two threads on the same follower connection**

- Scenario: Two threads on the same follower connection both call `conn.reconfigure('disaggregated=(checkpoint_meta="...")')` simultaneously with the same checkpoint metadata. Verify no crash, no corruption, and that the final state matches the checkpoint.
- Risk: `__disagg_pick_up_checkpoint_meta` holds the checkpoint lock during the operation, so concurrent calls should be serialised. However, no test verifies this under concurrency. If the locking is incomplete, two threads could both pass the LSN ordering check and execute the full `__disagg_apply_checkpoint_meta` sequence twice, potentially double-applying stable btree creations.
- Suggested test: Use `threading.Thread` to fire two simultaneous `reconfigure` calls on the same follower; assert one succeeds (with the idempotent log message for duplicates) and neither crashes.

---

**Gap 12 [LOW]: `disagg_advance_checkpoint` with a checkpoint whose stable timestamp is older than the follower's current stable timestamp**

- Scenario: Follower has picked up checkpoint at stable=T2. Attempt to advance to a checkpoint with stable=T1 (T1 < T2) — i.e., attempt to regress the follower.
- Risk: The page-log LSN ordering check prevents picking up an older checkpoint by LSN. But if two checkpoints have different stable timestamps but increasing LSNs (possible if stable timestamp is set non-monotonically on the leader), the follower could accept a checkpoint with a lower stable timestamp. No test verifies that this is rejected or handled gracefully.
- Suggested test: Extend `test_layered53` with an attempt to advance to a checkpoint with lower stable timestamp and assert the expected behavior (rejection or idempotent).

---

### Named Checkpoints

**Covered:** Nothing. All named checkpoint calls are intercepted and the test is skipped by `hook_disagg.py` line 249–250: `if 'name=' in config: skip_test('named checkpoints do not work in disagg storage')`.

---

**Gap 13 [DEFERRED]: No negative test for named checkpoints**
*(Tracked as NC-1 in 08_unsupported_features.md.)*

- Scenario: Open a disagg connection directly (not through the hook). Call `session.checkpoint('name=my_checkpoint')`. Assert the call returns a specific error code and that no partial data is written.
- Risk: Without a negative test, the behavior of the implementation when a named checkpoint is requested is entirely unspecified from a test perspective. The hook skip prevents any named-checkpoint path from being exercised, meaning that if the skip logic is ever removed or the underlying implementation is changed, there is no regression protection.
- Suggested test: New `test_layered_named_checkpoint_error` — direct test (marked `disagg_only`) that explicitly tests the named-checkpoint error path without going through the hook. Expected behavior: a clean `EINVAL` with a message like "named checkpoints not supported in disaggregated storage".

---

### Concurrent/Abrupt Scenarios

**Covered:**
- Connection close during step_up: `test_layered27` uses `conn.close('debug=(skip_checkpoint=true)')` after step_up on the old leader, which tests that closing without a final checkpoint leaves the page log in a consistent state for the new leader.
- Crash-restart (simulate_crash_restart): `test_layered87` simulates a crash on a disagg leader and verifies RTS does not roll back disagg data at recovery.
- step_down while eviction runs: `test_layered77` — eviction of pages with pending split state during leader→follower transition.

---

**Gap 14 [HIGH]: Connection close during drain worker execution (multithreaded drain)**

- Scenario: Configure the connection with `disaggregated=(drain_threads=4)` to use multiple drain worker threads. Start a step_up on a follower with large ingest data (to make drain take time). Close the connection (or call `conn.close()`) while the drain worker threads are running.
- Risk: `__wti_layered_drain_ingest_tables` at `conn_layered_ingest.c:693` signals worker threads to stop by setting `running=false` and then calls `__wt_thread_group_destroy`. If the connection close races with the drain workers completing their last work item, there may be a window where `__wt_thread_group_destroy` is called while a worker thread is still executing `__layered_drain_worker_run`. No test exercises this close-during-multithreaded-drain path — all Python tests use the default single-threaded drain.
- Suggested test: Configure `drain_threads=4`, open a large follower with enough ingest data to require multiple seconds of drain, start step_up from a separate thread, and close the connection after a short delay. Verify that `conn.close()` returns cleanly and that subsequent cold restart recovers the correct state.

---

**Gap 15 [HIGH]: Connection close during step_up (before drain completes)**

- Scenario: A connection has just transitioned to leader (step_up in progress), and the process calls `conn.close()` before the drain of the ingest tables has completed. Since step_up holds the checkpoint lock, and `conn.close()` also needs the checkpoint lock, one of the two must wait. Verify that whichever waits does so correctly and that neither panics.
- Risk: `__disagg_step_up` sets `WT_CONN_RECONFIGURING_STEP_UP` and holds the checkpoint lock for the duration. If `conn.close()` is called concurrently on a different thread, it will block on the checkpoint lock. On release, `close()` will see a partially-completed step_up (leader=true, drain incomplete). No test exercises this specific race.
- Suggested test: Use `threading.Thread` — one thread calls `step_up`, another calls `conn.close()` shortly after. Verify the connection reaches a clean state (either close wins and the connection is cleanly shut down, or step_up completes and then close proceeds normally).

---

**Gap 16 [LOW]: Crash during drain (abrupt process termination, not clean close)**

- Scenario: Simulate a process crash (e.g., via `simulate_crash_restart`) while the drain worker is actively copying ingest table data to the stable btree, at a point when some tables have been fully drained but others have not yet started.
- Risk: After a crash mid-drain, the in-memory ingest data is lost, but the stable btree may have been partially updated. On restart as a follower, the node picks up the last complete checkpoint from before the drain. If that checkpoint is internally consistent, this is safe; but if `__disagg_begin_checkpoint` had already started the new checkpoint epoch (before drain completes, at step_up line 1293), a partial checkpoint epoch record may exist in the page log. No test verifies crash recovery from this state.
- Suggested test: Add a `debug_mode` fault injection point inside `__layered_copy_ingest_table` that crashes after draining exactly N tables. Verify cold restart as a follower recovers the pre-step_up state.

---

## Priority-Ranked Gap List

### CRITICAL

_(No currently actionable CRITICAL items — step-down-dependent gaps are DEFERRED; see `08_unsupported_features.md`.)_

### HIGH

1. **Gap 9** — Follower calls `disagg_advance_checkpoint` before the leader has ever checkpointed — missing test for the startup race condition where a follower comes online before the first leader checkpoint.

2. **Gap 1** — Checkpoint immediately after step_up on a connection that just picked up a follower checkpoint and has no new data — interaction between step_up `__disagg_restart_checkpoint` and a clean-state checkpoint.

3. **Gap 2** — `session.checkpoint('force=true')` disagg-specific semantics are untested; one incidental call in `test_layered63` does not validate force semantics.

4. **Gap 14** — Close connection during multithreaded drain (drain_threads > 1) — no Python test uses `drain_threads > 1`; the entire multithreaded drain code path has zero Python test coverage.

5. **Gap 15** — `conn.close()` concurrent with in-progress step_up — checkpoint lock contention between close and step_up completion path.

### MEDIUM

6. **Gap 6** — step_up when follower ingest table has only uncommitted data — correctness of drain's non-prepared uncommitted update handling.

7. **Gap 7** — step_up failure path triggers `__wt_panic` — behavior is design-intentional but entirely untested; no test verifies that a panicked step_up results in `WT_PANIC` return and connection unusability.

8. **Gap 3** — `session.checkpoint()` on follower — partial test exists but doesn't assert return value or confirm no page-log write was attempted.

9. **Gap 10** — Follower advances checkpoint while two concurrent reader snapshots are open — multi-reader history-store pinning stress.

10. **Gap 11** — Two threads on same follower connection both call `disagg_advance_checkpoint` simultaneously — checkpoint lock serialisation under concurrency.

### LOW

11. **Gap 5** — `force_stop` checkpoint on a disagg leader — interaction with page-log checkpoint epoch not tested.

12. **Gap 8** — Two simultaneous leader connections on the same page log — no test for the dual-leader error path; behavior depends on page-log exclusivity guarantees that are currently implicit.

13. **Gap 12** — Follower `disagg_advance_checkpoint` with a checkpoint at a lower stable timestamp than the current follower stable timestamp — LSN ordering check is tested, but stable timestamp regression is not.

14. **Gap 16** — Crash mid-drain (abrupt process termination after partial drain completes) — requires fault injection infrastructure not currently present in the Python test suite.

### Deferred — Named Checkpoints and Step-Down (Target: Public Preview)

*(Gap 4 and Gap 13 — named checkpoint negative tests — are tracked as NC-1 in `08_unsupported_features.md`.)_
*(CP-1 (SD-1), CP-2 (SD-2), CP-3 (SD-3) — step_down → step_up without pickup, rapid step_down + step_up, and multi-hop transitions — are DEFERRED; see `08_unsupported_features.md` for details.)*

### DEFERRED — Requires Elegant Step-Down (Target: Public Preview)

CP-1 (SD-1), CP-2 (SD-2), CP-3 (SD-3) — see `08_unsupported_features.md` for details.
