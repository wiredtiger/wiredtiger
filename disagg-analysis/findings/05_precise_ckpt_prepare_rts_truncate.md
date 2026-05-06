# Gap Analysis: Precise Checkpoints / Prepared Transactions / RTS / Truncate
Generated: 2026-05-06

## Scope

Covers 24 Jira tickets from `/tmp/agent_g5.txt` related to precise checkpoints,
prepared transactions in disagg, rollback-to-stable (RTS) interaction with disagg,
and truncate operations in disagg.

Previously known gaps (PT-1 through PT-5, RTS-1 through RTS-5, FT-1, SO-H1 through
SO-H3) are excluded from this report per instructions.

---

## Confirmed Testing Gaps

### [WT-14497] — Precise checkpoint behavior when no stable timestamp is set

**Status:** Open | **Priority:** P3

**Gap:** No test explicitly validates the error-or-content policy when a precise
checkpoint is requested but `stable_timestamp` has never been set.  The ticket
documents two divergent behaviours (write nothing vs. write all content) and the
resolution—likely returning `WT_EINVAL`—is still undecided.  The comment from
Alexander Gorrod ("My preference is to return an error") confirms the policy is
unresolved, and until it is there can be no regression test.

**Test needed:**
- A Python-suite test (or csuite) that opens a disagg connection with
  `precise_checkpoint=true`, issues a checkpoint *before* any
  `set_timestamp('stable_timestamp=...')`, and asserts the specific expected
  outcome (error or defined snapshot content).
- A variant that sets `stable_timestamp` *after* writing data but *before* the
  checkpoint fires, verifying the late-set stable timestamp is respected.

**FIXME in source:** `FIXME-WT-14721` in `src/conn/conn_api.c:3449` notes that
disagg should enforce precise checkpoint but mongod is not ready; further validates
that this path has no automated test guard yet.

---

### [WT-14830] — Stress testing for prepared atomicity

**Status:** Open | **Priority:** P3 | Story Points: 5 (defined pipeline)

**Gap:** Ticket explicitly calls for adding preserve-prepared testing to `test/format`
and `test/checkpoint` to verify atomicity of prepared transactions under precise
checkpoints.  No such stress test exists today (the ticket is in the "defined
pipeline" backlog with no PR).

**Test needed:**
- `test/format` with `ops.prepare=1` + `precise_checkpoint=true` in disagg leader
  mode, running a concurrent workload that interleaves prepares/commits/rollbacks
  with frequent checkpoints.
- A `test/checkpoint` variant that verifies that after crash-recovery, no
  partially-applied prepared transaction is visible.

---

### [WT-14902] — No test for RTS-before-precise-checkpoint reconfiguration

**Status:** Backlog | **Priority:** P3

**Gap:** Ticket scope is to (a) decide whether precise checkpoint should be runtime-
reconfigurable, and (b) run RTS before the first precise checkpoint if
reconfiguration is allowed.  There is currently no test that exercises the
reconfiguration path at all, and no negative test that proves reconfiguring
`precise_checkpoint` without prior RTS is rejected or safe.

**Test needed (contingent on design decision in ticket):**
- If reconfiguration is allowed: a test that reconfigures `precise_checkpoint=true`
  at runtime, verifies RTS ran, and confirms the first subsequent checkpoint is
  precise-compliant.
- If reconfiguration is disallowed: a test that attempts the reconfigure call and
  asserts `WT_EINVAL` or similar.

---

### [WT-15227] — Python suite disagg hook does not enable precise checkpoints

**Status:** Open | **Priority:** P3 | Story Points: 8

**Gap:** The disagg hook (`hook_disagg.py`) does not set `precise_checkpoint=true`
for Python test runs.  This means that the majority of Python-suite tests run
against disagg *without* the key constraint that defines disagg correctness.  Any
test that happens to pass today under the hook may silently be testing a
weaker-than-production configuration.

The confirmed gap is structural: a large class of existing layered/disagg Python
tests (those that do not set stable timestamps) cannot use precise checkpoints
without modification.

**Test needed:**
- Resolution of the hook design (e.g., add a flag argument, or restrict to
  timestamp-using tests as discussed).
- Once resolved, regression coverage confirming all tests that *should* exercise
  precise checkpoints actually do so.

---

### [WT-15294] — test_prepare20.py crashes in checkpoint under disagg hook

**Status:** Open | **Priority:** P3

**Gap:** Running `test_prepare20.py` under `--hook disagg` reliably crashes in
`rec_hs.c` during checkpoint reconciliation (history-store cursor position
assertion).  The test is on the `hook_disagg.fail` blocklist, so it is NOT
currently executed in Evergreen disagg runs.  This is a zero-coverage gap: the
prepare-checkpoint interaction exercised by scenario 9 of this test is untested in
disagg.

**Test needed:**
- Fix the abort, remove from `hook_disagg.fail`, and confirm test_prepare20 passes
  under disagg.

---

### [WT-15397] — Truncate disabled when precise checkpoint + prepare both on

**Status:** Open | **Priority:** P3

**Gap:** `test/format/format_config.c:1548` contains:
```c
/* FIXME-WT-15565 Write prepared truncate operation to disk. */
if (GV(PRECISE_CHECKPOINT) && GV(OPS_PREPARE)) {
    config_off(NULL, "ops.truncate");
}
```
This means `test/format` in disagg mode silently suppresses all truncate operations
whenever prepared transactions are also enabled.  The combination
`precise_checkpoint + prepare + truncate` is therefore entirely untested.
The workaround is intentional pending WT-15565 (write prepared fast truncate to
disk), but no negative test confirms the correct error is returned if an application
attempts truncate in this combination.

**Test needed:**
- Once WT-15565 is resolved, remove the `config_off` guard and re-enable truncate +
  prepare in `test/format` disagg configs.
- A targeted Python test that issues a cursor truncate inside a prepared transaction
  in disagg mode and verifies either success (if permitted) or the correct error
  code (if still blocked).

---

### [WT-15475] — Truncate "Invalid argument" in leader mode (test/format)

**Status:** Open | **Priority:** P3

**Gap:** `test/format` in `disagg.mode=leader` produces a spurious "start cursor is
after stop cursor" error on truncate (`EINVAL`).  There is no test that explicitly
covers cursor-based range truncate on a leader where the cursor positions may
diverge from follower expectations.

This is distinct from SO-H2 (truncate on leader vs follower): this particular gap
is about cursor positioning semantics on the leader.

**Test needed:**
- A regression test that performs cursor-range truncate on a leader and verifies the
  cursor ordering is consistent with data visible at the leader's read timestamp.

---

### [WT-15552] — precise_checkpoint hardcoded in test_util.h; no validation

**Status:** Open | **Priority:** P3 | Story Points: 3

**Gap:** `test_util.h` unconditionally forces `precise_checkpoint = true` for any
disagg test.  Acceptance criteria require (a) removing the hardcode, (b) exposing
it as an explicit config option in `test/format` and `test/checkpoint`, and (c)
adding validation that fails loudly if `precise_checkpoint` is *not* set in disagg
mode rather than silently overriding it.

Until this is done, test infrastructure cannot distinguish "test explicitly opts
into precise checkpoint" from "test inherits it by accident," obscuring coverage.

**Test needed:**
- Once the hardcode is removed: confirm that a disagg test run without
  `precise_checkpoint=true` in its config fails with an explicit diagnostic, not a
  silent override.

---

### [WT-15565] — No test for prepared fast-truncate persistence (disk format)

**Status:** Open | **Priority:** P3 | Story Points: 8

**Gap:** Prepared fast-truncate operations are not written to disk with a prepared-id
encoding.  The `test/format` workaround (`config_off("ops.truncate")`) masks this.
There is no test that verifies the on-disk representation of a prepared fast truncate
survives crash-recovery correctly.

**Test needed:**
- A test that performs a fast truncate inside a prepared transaction, checkpoints,
  simulates a crash, restarts, and verifies the prepared truncate is either:
  (a) correctly rolled back (if not committed), or
  (b) correctly committed on recovery.
- Must run in disagg leader mode with `precise_checkpoint=true`.

---

### [WT-16259] — No test for prepared transactions across checkpoint updates on standbys

**Status:** Backlog | **Priority:** P3

**Gap:** The ticket raises open questions: Do standbys execute prepared transactions?
In a step-up scenario, how are in-flight prepares handled when a follower becomes
leader?  Should prepared transactions be allowed to span checkpoint pick-ups?  None
of these scenarios have any test coverage.

**Test needed:**
- A multi-node test/format or Python test that starts a prepared transaction on a
  leader, performs a checkpoint, switches the follower to leader (step-up), and
  verifies the prepared transaction is correctly handled (rolled back or preserved).
- A test that verifies the behaviour when `oldest_timestamp` advances past a
  prepared-but-uncommitted transaction in disagg mode (WT-16258 context).

---

### [WT-16732] — No predictable-replay test for truncate in multi-node disagg

**Status:** Open | **Priority:** P3 | Story Points: 5

**Gap:** Truncate is not supported in predictable replay, which means `test/format`
multi-node disagg runs (leader + follower in parallel) never exercise truncate.
This is a significant blind spot: any divergence between leader and follower
truncate behaviour goes undetected.

The last comment (Jie Chen, 2026-03-30) notes this can be partially deferred to
switch mode coverage, but switch mode itself does not guarantee the same multi-node
concurrency that production exercises.

**Test needed:**
- Extend predictable replay to record and replay truncate operations.
- Add a multi-node `test/format` configuration that exercises concurrent truncate
  with checkpoint under precise_checkpoint=true.

---

### [WT-16813] — No test for truncate-list GC on follower checkpoint pick-up

**Status:** In Progress | **Priority:** P3 | Sprint: active (2026-05-08)

**Gap:** WT-16813 ticket description explicitly calls for "a functional test to
verify that obsolete entries are removed while active/visible truncates remain
intact" after checkpoint pick-up on the follower.  The PR is open; no such
functional test exists in the suite yet.

**Test needed:**
- A Python test (or csuite) that:
  1. Performs multiple truncate operations on a layered table in follower mode.
  2. Takes a checkpoint.
  3. Simulates checkpoint pick-up on the follower (advances prune timestamp).
  4. Verifies that truncate entries older than the checkpoint are pruned from the
     in-memory list, and entries that span the checkpoint boundary remain.

---

### [WT-16961] — No test for "best-effort" truncate config option

**Status:** Open | **Priority:** P3

**Gap:** The ticket explicitly states: "We'll still need an ad hoc test to verify
that `best-effort` works."  The `best_effort=true` config option for
`WT_SESSION::truncate()` is not yet implemented (ticket is open), but the
acceptance path explicitly requires a Python test.  There is zero coverage today.

**Test needed:**
- A Python test that:
  1. Opens a layered table without fast-truncate support.
  2. Issues `session.truncate(uri, start, stop, "best-effort=true")`.
  3. Verifies WT returns success even if not all records were truncated.
  4. Verifies partial truncation is handled correctly (no data corruption, no crash).

---

### [WT-17135] — Enabling follower fast truncate on develop requires test validation

**Status:** Open | **Priority:** P3 | Sprint: active (2026-05-08)

**Gap:** This ticket gates on "follower mode and step-up phase working" before
removing feature flags.  There is no test that exercises the step-up phase with
fast truncate active—i.e., a follower that has pending truncate list entries
becoming a leader and processing those entries correctly.

**Test needed:**
- A test that:
  1. Issues fast truncates on a follower node.
  2. Triggers a step-up (follower becomes leader).
  3. Verifies truncated ranges are not visible on the new leader.
  4. Verifies data outside the truncated range is intact.

---

### [WT-17330] — No performance benchmark for truncate list traversal

**Status:** Open | **Priority:** P3

**Gap:** The layered-table truncate list is a flat linked list with O(N) traversal.
WT-16789 added statistics (traversal counts, search call counts), but no benchmark
test verifies those statistics stay within acceptable bounds under load.

**Test needed:**
- A performance-oriented test (or a statistics-assertion test) that:
  1. Performs N truncate operations.
  2. Reads `WT_STAT_*` truncate-list traversal statistics.
  3. Asserts the traversal count is bounded (e.g., does not grow faster than O(N)
     after GC).

---

### [WT-17377] — No test for durable_timestamp > prepare_timestamp enforcement

**Status:** Open | **Priority:** P3

**Gap:** Ticket requires a new test that explicitly covers:
- Setting `durable_timestamp == prepare_timestamp` on a prepared transaction →
  expects `WT_EINVAL`.
- Setting `durable_timestamp > prepare_timestamp` → expects success.
- Updating existing tests that inadvertently use equal timestamps.

No such test exists today.

**Test needed:**
- A Python-suite test in the prepare test series (e.g., `test_prepare37.py`) or a
  standalone csuite test that covers both the rejection and acceptance cases.

---

### [WT-17380] — prepare disabled in disagg switch mode on mainline

**Status:** Open (PR merged) | **Priority:** P3

**Gap:** `ops.prepare=0` is currently hardcoded in all switch-mode format test
configurations in `test/evergreen.yml` (via `FIXME` comments) because
prepare+switch-mode has unresolved failures.  This means the switch-mode variant
of disagg—which exercises leader↔follower transitions—never tests prepared
transactions.

**Test needed:**
- Identify and fix the root cause of prepare failures in switch mode (likely related
  to WT-15294, WT-15565, or WT-15397).
- Remove `ops.prepare=0` from switch-mode configurations.
- Confirm switch-mode Evergreen variants run with prepare enabled.

---

### [WT-14361] — test_truncate16 flaky in non-disagg (fast-delete pages assertion)

**Status:** Open (Build Failure) | **Priority:** P3

**Gap:** `test_truncate16` asserts `fastdelete_pages > 0` after a checkpoint
truncate.  This assertion fails intermittently when fast-delete pages are not
produced.  This is a non-disagg failure, but the auto-resolution rule explicitly
excludes disagg variants (`projects: ^((?!disagg).)*$`), meaning the same scenario
in disagg is untested.

**Test needed:**
- Once the non-disagg root cause is understood, validate whether precise checkpoints
  in disagg alter the fast-delete page count expectation (they likely do, since
  precise checkpoints may force page reads-back that eliminate fast-delete
  opportunities).

---

### [WT-16276] — test_cursor18 prepared value assertion error (Disagg label)

**Status:** Backlog (Build Failure) | Label: Disagg

**Gap:** `test_cursor18.test_prepare_tombstone` fails with a prepared-state
assertion error (`expected_prepare_state = 1, got 0`) on `wiredtiger-mongo-v7.0`.
The ticket is labeled `Disagg`.  There is no coverage confirming prepared tombstones
are visible/invisible correctly through a disagg checkpoint.

**Test needed:**
- Investigate whether the failure is specific to the disagg checkpoint path.
- Add a targeted test for prepared tombstone visibility after precise checkpoint in
  disagg mode.

---

## Uncertain Cases

### [WT-14523] — Fast truncate performance with disagg (no test, by design)

**Status:** Open

The ticket studies whether frequent checkpoints in disagg cause performance
regressions when truncates overlap with checkpoints (requiring page read-backs).
The last comment (Alexander Gorrod) notes this is critical for OpLog truncation
performance.

This is primarily a **performance investigation** rather than a functional test gap.
However, there is no micro-benchmark that measures the cost of a truncate operation
that overlaps a precise checkpoint.  Whether this belongs in the testing gap
analysis depends on whether a performance regression test is expected.

Tentative classification: **uncertain** — needs a decision on whether to add a
performance regression test alongside the functional path.

### [WT-14879] — Page delta support for fast truncates (Backlog)

**Status:** Backlog | Priority: P4

The FIXME at `src/reconcile/rec_child.c:239` reads:
```c
/* FIXME-WT-14879: support delta for fast truncate. */
```
This means deltas are not generated for fast-truncate pages.  Testing impact is
limited until the feature is implemented.  No test gap exists today since the
feature path is disabled.

### [WT-14630] — Eviction constraints under precise checkpoints (investigation)

**Status:** Open

This is a performance investigation (CPU-bound eviction starving clean eviction
under 256 threads + precise checkpoints).  The finding (increase eviction threads
from 4 to 6) has been documented.  No functional test gap exists, but no
regression test guards against this performance cliff.  Classification: **uncertain
performance gap**.

### [WT-15040] — Enable prepared transactions in test/model (depends on PT series)

**Status:** Open

Explicitly blocked on prepared transaction implementation.  Aligned with known
PT-1 through PT-5 deferred tests.  No incremental gap beyond what is already noted.

### [WT-15081] — Support prepared fast-truncate in disagg (depends on WT-15565)

**Status:** Open

This is the parent-design ticket for WT-15565.  The test gap for prepared
fast-truncate persistence is captured under WT-15565 above.

---

## No Gap (Notes)

### [WT-14998] — Re-enable layered tables on truncate tests

The ticket tracks re-enabling disagg on standard Python truncate tests.  Progress
is ongoing (Jie Chen, assignee).  The gap is a known tracking item, and
corresponding tests exist but are disabled rather than missing.

### [WT-16276] — test_cursor18 (non-disagg root cause)

Included under Confirmed Gaps above because of the `Disagg` label, but the
immediate failure is on `wiredtiger-mongo-v7.0` (non-disagg config).  Root cause
investigation needed before disagg-specific gap can be confirmed.

---

## New Testing Areas (Identified from FIXME Scan)

The source FIXME scan (`src/`) revealed three additional code-level markers that
have no corresponding test coverage and are disagg-relevant:

1. **`FIXME-WT-14721` (`src/conn/conn_api.c:3449`)** — "Disaggregated storage
   should only support precise checkpoint but mongod is not ready for that yet."
   No test verifies the enforcement path (or the absence of enforcement).
   Recommended: a test that opens disagg without `precise_checkpoint=true` and
   verifies either a warning or silent acceptance (whatever the policy becomes).

2. **`FIXME-WT-14739` (`src/txn/txn.c:2606`)** — Shutdown checkpoint for
   followers.  No test exercises shutdown under follower mode with pending prepared
   transactions.

3. **`FIXME-WT-16562` (`src/conn/conn_layered.c:725`)** — Checkpoint size tech
   debt.  No test validates disagg checkpoint metadata size bounds.

4. **`src/btree/bt_sync_obsolete.c:453`** — "Read internal pages from non-logged
   tables when the remove/truncate..."  No test exercises truncate on a non-logged
   (in-memory) table in disagg mode.
