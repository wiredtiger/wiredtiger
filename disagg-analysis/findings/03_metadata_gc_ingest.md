# Gap Analysis: Shared Metadata / History Store / Ingest GC / Checkpoint Pickup
Generated: 2026-05-06

Tickets analysed: 24 (all entries from /tmp/agent_g3.txt).
Source material: Jira descriptions/comments, FIXME annotations in `src/`, existing
test coverage (`test/suite/test_layered*.py`), and prior analysis in
`test/analysis/05_scenario_analysis/`.

---

## Confirmed Testing Gaps

### [WT-14521] — Investigate if transaction IDs need consideration during ingest GC

**Type/Priority/Status:** Task / Major-P3 / Open (unassigned)
**Jira context:** The ticket asks two questions: (1) on standby, is it enough to
check global visibility of a transaction ID before GC-ing an ingest key? (2) during
step-up drain, can we skip the txn-ID check for keys already moved to stable?
Neither question has a code answer yet, and no test exercises the boundary.
**Gap:** No test confirms that a GC attempt on an ingest key whose owning transaction
is *not yet globally visible* correctly retains the key.  Equally there is no test
for the step-up drain fast-path (keys moved to stable can be GC'd without txn-ID
check).
**Suggested test:** `test_layered_gc_txnid_visibility` — (a) open two concurrent
readers on a follower, one of which pins the oldest active transaction; advance
`oldest_timestamp` to a point that would allow GC, but keep the pinning transaction
open; verify that the ingest key is *not* GC'd.  (b) Complete the step-up drain and
confirm the key is removed from ingest once it is in stable.
**Already in existing analysis?** No — NEW GAP (TT-GC1)

---

### [WT-14913] — Implement coherence verification for ingest and stable tables

**Type/Priority/Status:** Task / Major-P3 / Backlog (SE-Persistence)
**Jira context:** Extends `session->verify()` to check ingest↔stable coherence on
the follower (primary-mode version is WT-14911/WT-14912).  Ticket says "new tests if
needed"; none exist yet.  Story points = 8, sitting in backlog.
**Gap:** `verify()` on a layered table does not currently check that every key in the
ingest table either has a matching entry in stable or is genuinely new (not yet in a
checkpoint).  Existing test SO-M2 (`test/analysis/05_scenario_analysis/07_verification.md`)
calls `verify()` on a table that has only ingest data but does not check
ingest↔stable coherence.
**Suggested test:** `test_layered_verify_coherence` — write data through a leader
checkpoint cycle so some keys are in stable and some remain in ingest, then on the
follower call `session.verify()` and assert it passes; then deliberately corrupt an
ingest entry (value mismatch vs stable) and assert `verify()` returns an error.
**Already in existing analysis?** Partially — SO-M2 covers the basic verify call
but NOT the coherence cross-check.  The coherence aspect is a NEW GAP (V-GC1).

---

### [WT-15476 + WT-17189 + WT-17192] — Validate layered table content during GC (epic + two subtasks)

**Type/Priority/Status:** Epic / Major-P3 / Open (Jasmine Bi, due 2026-05-15).
WT-17189 (debug-build verification) has an open PR; WT-17192 (release-build
probabilistic check) is open with no PR.
**Jira context:** The epic asks that at GC time WiredTiger verify that the
most-recent-update about to be pruned either (a) exists and matches in stable (for
data updates) or (b) does not exist in stable (for tombstones).  WT-17189 is being
implemented for diagnostic builds now.  The latest comment on WT-17189 describes
several subtle bugs found during implementation: lazy page-fetch races,
`\x14\x14` delete-sentinel misidentification, and `preserve_prepared` interactions.
**Gap:** No *functional test* yet validates the verification itself — i.e., a test
that deliberately inserts a mismatch and confirms the debug assertion fires.
WT-17192 (release-build probabilistic sampling) has no test design at all.
**Suggested tests:**
- `test_layered_gc_verify_debug` — enable `diagnostic` build; write key K at ts=10
  on leader, checkpoint, verify K is in stable; on follower advance checkpoint and
  let GC run; confirm GC statistic increments and no panic.
- Negative variant: corrupt the stable table entry after checkpoint but before GC
  runs and confirm the diagnostic assertion catches it.
- `test_layered_gc_verify_release` — run 1-in-N sampling (WT-17192) for 10 k GC
  operations; confirm sampling rate stat matches expected frequency.
**Already in existing analysis?** No — NEW GAP (V-GC2 / V-GC3).

---

### [WT-15591] — Ensure WT_IS_METADATA checks also cover disagg shared metadata

**Type/Priority/Status:** Task / Major-P3 / Backlog (SE-Foundations)
**Jira context:** Many code paths guard themselves with `WT_IS_METADATA(dhandle)`.
With the shared metadata table (`WiredTigerShared.wt_stable`) these guards
are incomplete.  The last comment proposes a combined macro.  No fix committed.
**Gap:** No test exercises a code path that is incorrectly conditioned on only the
local metadata.  The consequence is silent wrong behaviour (e.g. an operation that
should be skipped for the shared metadata runs on it, or vice versa).  A regression
test that triggers each code path in disagg mode on the shared metadata table would
catch such omissions.
**Suggested test:** `test_layered_shared_metadata_ops` — attempt the operations
known to check `WT_IS_METADATA` (e.g. `session.verify()`, compact, backup) on a
connection configured with disagg; assert each either succeeds correctly or returns
the expected error, and does not crash.
**Already in existing analysis?** No — NEW GAP (SM-1).

---

### [WT-16188] — Checkpoint pick-up scales to millions of tables (N² complexity)

**Type/Priority/Status:** Task / Major-P3 / Open (SE-Foundations, Milestone Disag_M15).
**Jira context:** Checkpoint pickup currently iterates the *entire* shared metadata
table and copies/updates every entry to local metadata — O(N) per pickup, O(N²) in
total over many pickups.  A real-world case of a 52-minute startup was logged
(HELP-88868).  Two design options are described (union cursor, or change-log in shared
metadata).
**Gap:** There is no performance test or scale test for checkpoint pickup with a large
table count.  No functional test validates that pickup completes correctly and in
bounded time when N > a few hundred tables.  There is also no test for any partial
implementation of the change-log design (once implemented).
**Suggested test:**
- `test_layered_checkpoint_pickup_scale` — create 500+ collections on a leader,
  checkpoint, have a follower pick up; assert pickup completes in < X seconds and all
  collection data is readable on the follower.
- After the change-log design lands: test that the follower uses the log path (not
  full scan) when the log covers the required history.
**Already in existing analysis?** Partially — CP-H1 in `05_checkpoint_roles.md`
covers "before first checkpoint" but not scale. This is a NEW dimension: CP-SCALE.

---

### [WT-16257] — Add oldest_timestamp to checkpoint metadata

**Type/Priority/Status:** Task / Major-P3 / Backlog (SE-Transactions).
Linked dependency: SERVER-115340 (snapshot-read test coverage for disagg).
**Jira context:** For point-in-time reads on followers, the follower must know the
leader's `oldest_timestamp` at the time of each checkpoint.  Without this, a
follower cannot correctly reject transactions requesting data older than the leader's
GC horizon.  This is a prerequisite for PIT reads.
**Gap:** No test verifies that a follower correctly uses an `oldest_timestamp`
embedded in checkpoint metadata to reject (or allow) read-timestamp transactions.
TT-H1 (`test_layered_timestamps01.py`) covers the case where the *follower's own*
`oldest_timestamp` has advanced, but not where the follower inherits the leader's
`oldest_timestamp` from checkpoint metadata.
**Suggested test:** `test_layered_oldest_ts_from_checkpoint` — leader sets
`oldest_timestamp=50`, checkpoints; follower picks up the checkpoint; follower
attempts `begin_transaction(read_timestamp=30)`; assert WT_NOTFOUND or appropriate
error because the leader's GC boundary was 50.
**Already in existing analysis?** TT-H1 is related but covers a different scenario.
This is a NEW GAP (TT-H3) for cross-node oldest_timestamp propagation.

---

### [WT-16477] — Read shared metadata directly on standby (avoid checkpoint lock)

**Type/Priority/Status:** Improvement / Major-P3 / Open (unassigned).
**Jira context:** On a follower, opening a shared table's dhandle currently takes
the checkpoint lock to read consistent HS+data-store checkpoint info from the local
metadata.  The FIXME comment is present in `src/btree/bt_handle.c:210`.  The proposed
fix reads directly from `WiredTigerShared.wt_stable` to avoid the lock.
**Gap:** No test exercises the lock-contention scenario: a follower session tries to
open a table dhandle *while* a checkpoint pickup is in progress on another thread.
If the lock is removed prematurely (before the fix is complete), reads could see
inconsistent HS/data checkpoint pairs.
**Suggested test:** `test_layered_open_dhandle_during_pickup` — use two threads on a
follower: one continuously calling `disagg_advance_checkpoint`, the other opening
and reading from a shared table in a loop; assert no WT_PANIC and that every read
returns consistent data (HS + data store at the same checkpoint epoch).
**Already in existing analysis?** No — NEW GAP (SM-2).

---

### [WT-16813] — GC checkpoint pick-up with fast truncate design (follower)

**Type/Priority/Status:** Task / Major-P3 / In Progress (Krishen Chovhan, active sprint ending 2026-05-08).
**Jira context:** The fast-truncate design maintains a linked-list of truncate ranges
between ingest and stable.  Without GC at checkpoint pickup, followers accumulate an
ever-growing truncate list, degrading cursor performance.  The ticket description
explicitly requires a functional test verifying that obsolete entries are removed
while active/visible truncates remain.
**Gap:** The ticket is in progress but the functional test has not yet been written
(no PR merged as of 2026-05-06).  No existing `test_layered_fast_truncate*.py` test
covers GC of the truncate list at checkpoint pickup.
**Suggested test:** `test_layered_fast_truncate_gc` (as specified in the ticket) —
issue multiple fast truncates on a follower, advance several checkpoints, verify that
truncate-list entries whose range is fully covered by the new stable table are
removed; verify that truncates that are still partially in-flight remain in the list.
**Already in existing analysis?** No — NEW GAP (FT-GC1).

---

### [WT-17040] — Investigate whether shared metadata creation is necessary on followers

**Type/Priority/Status:** Task / Major-P3 / Open (SE-Foundations).
**Jira context:** Followers currently create the shared metadata table in
`__disagg_metadata_table_init` and immediately expire the live dhandle as a
short-term workaround.  The FIXME comment in `src/conn/conn_layered.c:1105` directly
references this ticket.  The code comment says "investigate if necessary".  The
ticket notes `test_layered15` as the key validation test.
**Gap:** No test validates the scenario where a follower that has never written to the
shared metadata table performs a step-up, and that all schema operations accumulated
in local metadata are correctly promoted to shared metadata.  `test_layered15`
tests schema operations but not the specific shared-metadata-creation path for
followers.
**Suggested test:** `test_layered_follower_shared_meta_stepup` — open follower (no
writes), verify shared metadata table was initialised and is empty, step up, create
tables on the new leader, checkpoint, verify shared metadata now contains the
expected entries.
**Already in existing analysis?** No — NEW GAP (SM-3).

---

### [WT-17146] — Add shared metadata consistency check to verify()

**Type/Priority/Status:** Task / Major-P3 / Backlog (SE-Persistence).
Epic link: WT-16720.
**Jira context:** In disagg mode, table metadata lives in both the local metadata
table and `WiredTigerShared.wt_stable`.  The verify path does not yet cross-check
these two sources.  The ticket proposes: (a) for each entry in shared metadata,
confirm a matching local entry exists; (b) for each local file entry, confirm a
matching shared entry exists.
**Gap:** No test calls `verify()` in a way that would expose local↔shared metadata
divergence.  The existing `test/analysis/05_scenario_analysis/07_verification.md`
analysis identifies `verify()` as having partial disagg coverage but does not flag
the specific local↔shared consistency check as missing.
**Suggested test:** `test_layered_verify_shared_meta` — create tables, checkpoint,
artificially insert a table entry into local metadata that has no shared counterpart
(or vice versa), then call `session.verify()` and assert it returns an error
describing the inconsistency.  Positive case: consistent state → verify() passes.
**Already in existing analysis?** No — NEW GAP (V-SM1).

---

### [WT-17063] — Support shared disk hash table switch in node step-down/step-up

**Type/Priority/Status:** New Feature / Major-P3 / Open (SE-Transactions, active discussion).
**Jira context:** The shared disk hash table (used for the in-memory page cache) needs
to be correctly initialised on step-up (follower→leader) and destroyed or kept alive
(for warm-up) on step-down.  The latest comment discusses whether to keep it alive for
~10 minutes after step-down.
**Gap:** No test exercises the hash-table lifecycle across a step-up/step-down cycle,
specifically: that the hash table is empty/non-existent in follower mode, is created
correctly on step-up, and is handled correctly on step-down.
**Suggested test:** `test_layered_shared_cache_stepup_stepdown` — verify hash table
presence/absence at each role transition; verify no entries from the follower's
hash table are visible after step-up; and verify entries from the leader's hash
table are eventually cleaned up after step-down.
**Already in existing analysis?** No — NEW GAP (SC-1).

---

### [WT-17250] — Add validation test for shared disk cache

**Type/Priority/Status:** Task / Major-P3 / Open (SE-Transactions, unassigned).
**Jira context:** The ticket explicitly requests a test that runs WiredTiger with the
shared disk cache for 10-20 minutes and then walks the hash table to confirm no
dangling entries (refcount=0, or entries referencing evicted pages).  No such test
exists.
**Gap:** There is no long-running cache integrity validation test.  Memory leaks or
dangling references in the shared disk hash table are currently undetected by the
test suite.
**Suggested test:** `test_layered_shared_cache_validate` — as specified in the Jira
ticket; run mixed read/write workload for an extended period with shared cache
enabled; at end walk the hash table and assert no entries with refcount=0 or
referencing non-existent cache pages.
**Already in existing analysis?** No — NEW GAP (SC-2).

---

## Uncertain Cases

### [WT-14494] — Use dhandle flag instead of dhandle name to identify history store

**Type/Priority/Status:** Task / Major-P3 / Open (unassigned, SE-Foundations).
**Assessment:** This is a performance/correctness refactor.  The concern is that code
using string comparison to identify the HS dhandle may be slow or subtly wrong in the
shared-HS case (where the dhandle name differs from the local HS name).  There is no
dedicated test gap *for the refactor itself*, but if the fix is incomplete, callers
that special-case the HS could fail to recognise the shared HS and apply wrong
behaviour (e.g. skipping the shared HS during obsolete check).
**Assessment:** Uncertain — whether a test gap exists depends on whether existing GC
and MVCC tests exercise the shared HS dhandle identification path.  No existing test
explicitly checks the `WT_IS_HS` / flag-based identification in disagg mode.
**Recommendation:** Flag as worth a targeted code review; a small test that opens the
shared HS dhandle and confirms it is correctly identified as "history store" would
close this.

---

### [WT-15159] — Confirm delta reconciliation on history store

**Type/Priority/Status:** Improvement / Major-P3 / Open (SE-Transactions).
**Assessment:** The ticket investigates whether HS pages produce delta images or full
images during reconciliation.  Prior evidence showed 60-70% of full-image writes were
from the HS.  This is a *performance* investigation, not a correctness gap per se.
However, if the HS always produces full images in disagg, the shared HS will generate
significantly more write I/O to the page server than expected.  The ticket has been
moved back to an active sprint (SE Transactions - 2026-05-08) suggesting work is
imminent.
**Assessment:** Uncertain — not a test gap in the traditional sense but a missing
benchmark/performance assertion.  Recommend a stat-based test that asserts the
`rec_page_full_image_leaf` stat for the shared HS does not exceed a configured
threshold per workload.

---

### [WT-16982] — Long-term solution for layered dhandles and ingest table lifetime

**Type/Priority/Status:** Task / Major-P3 / Backlog.
**Jira context:** Short-term fix (WT-16974) already prevents the sweep server from
closing layered/ingest dhandles.  The FIXME comment at `src/conn/conn_sweep.c:104`
references this ticket.  The long-term work (tying layered dhandle lifetime to its
ingest dhandle) is not yet done.
**Assessment:** Uncertain — there is no test that validates the sweep server
*correctly skips* layered/ingest dhandles.  A regression test confirming the sweep
server does not close these would be valuable, but this is more of a correctness
guard than a functional gap.  Recommend a stress test that opens many layered tables
with ingest data, triggers the sweep server, then does a step-up and verifies all
data is preserved.

---

## No Gap (notes)

### [WT-14732] — Improvements when copying ingest table content
Refactor / Minor-P4 / Backlog.  This is a code quality ticket (removing a layering
violation and tombstone dependency in `wt_clayered_deleted`).  No new testing gap
arises from this; it is a prerequisite cleanup for future correctness work.

### [WT-14736] — Layered random cursors ignoring size of ingest table
Minor-P4 / Open.  Statistical/sampling bias in random cursor — not a correctness gap,
just sub-optimal sampling weight.  No functional test gap; the FIXME comment at
`src/cursor/cur_layered.c:2474` directly references this ticket.

### [WT-16837] — Investigate whether stat log server should process ingest tables on leader
Open / investigation ticket.  The immediate issue (EBUSY during verify due to stat log
server) was addressed in WT-16703 by switching to a non-exclusive cursor.  No
outstanding test gap.

### [WT-16851] — Eliminate need to create missing ingest btrees when loading a checkpoint
Open / Backlog.  Architecture ticket for inferring ingest btree metadata from the
layered table metadata.  The comment notes this may also solve WT-16544 (slow
checkpoint pickup).  No test gap until the implementation lands; at that point the
checkpoint pickup scale test (CP-SCALE above) would cover it.

### [WT-17049] — Avoid reopening the stable table for each operation on leader
Backlog / performance improvement.  No functional correctness gap.  Once implemented,
the regression test is: confirm that the stable cursor dhandle is not reopened between
operations when the dhandle has not changed.

### [WT-17131] — Follower layered cursors should not reopen unchanged stable table at pickup
Backlog / performance improvement.  Similar to WT-17049 but for followers.  No
functional correctness gap currently; the described "larger bump" is a performance
regression, not a data-loss scenario.

### [WT-17327] — Document the stable schema epoch
Open / Documentation.  Peter Macko assigned.  No testing gap — this is pure
documentation work for `src/docs/timestamp-global.dox`.

### [WT-17066] — Investigate and define shared disk hash table bucket size
Open / performance investigation.  The ticket is about choosing the right bucket count
during initialisation; the comment notes that for 256 GB cache more than 1 million
locks would be created.  No test gap until bucket-size configuration is finalised;
the SC-2 test (WT-17250) is the relevant validation.

### [WT-15970] — During step-up, fix layered cursors to wait for ingest drain
Open / Bug.  The description notes that `WT_CONN_RECONFIGURING_STEP_UP` flag should be
consulted before closing the ingest cursor.  This is a correctness bug, but the test
gap (cursor still reading ingest during drain) is already captured in Gap 6 of
`05_checkpoint_roles.md` (step_up with uncommitted ingest data).  Partially covered;
the ingest-wait aspect is an additional dimension to that existing gap.

---

## New Testing Areas Identified

The following testing areas emerge from this ticket batch that are not represented
in any prior scenario analysis document:

| ID     | Area                                         | Jira tickets        | Priority |
|--------|----------------------------------------------|---------------------|----------|
| TT-GC1 | GC safety under pinned transaction IDs       | WT-14521            | HIGH     |
| TT-H3  | Cross-node oldest_timestamp propagation      | WT-16257            | HIGH     |
| V-GC1  | ingest↔stable coherence in verify()          | WT-14913            | HIGH     |
| V-GC2  | GC-time ingest vs stable verification (debug)| WT-15476, WT-17189  | HIGH     |
| V-GC3  | GC-time probabilistic sampling (release)     | WT-17192            | MEDIUM   |
| V-SM1  | local↔shared metadata consistency in verify()| WT-17146           | HIGH     |
| SM-1   | WT_IS_METADATA coverage for shared metadata  | WT-15591            | MEDIUM   |
| SM-2   | dhandle open under concurrent checkpoint lock| WT-16477            | MEDIUM   |
| SM-3   | Shared metadata creation on follower + step-up| WT-17040           | MEDIUM   |
| CP-SCALE | Checkpoint pickup with large table count   | WT-16188            | HIGH     |
| FT-GC1 | Fast truncate list GC at checkpoint pickup   | WT-16813            | HIGH     |
| SC-1   | Shared disk cache lifecycle at step-up/down  | WT-17063            | MEDIUM   |
| SC-2   | Shared disk cache integrity validation       | WT-17250            | MEDIUM   |

---

## FIXME Annotations with Testing Implications

Key FIXMEs found in source that correlate to gaps above:

| Location | FIXME | Gap |
|---|---|---|
| `src/btree/bt_handle.c:210` | FIXME-WT-16477: read from shared metadata to avoid checkpoint lock | SM-2 |
| `src/conn/conn_layered.c:363` | FIXME-WT-14730: check other parts of metadata are identical | SM-1 |
| `src/conn/conn_layered.c:1105` | FIXME-WT-17040: investigate if shared metadata creation necessary on follower | SM-3 |
| `src/conn/conn_sweep.c:104` | FIXME-WT-16982: optimization to close layered dhandles with empty ingest | (performance; no direct gap) |
| `src/cursor/cur_layered.c:2474` | FIXME-WT-14736: consider size of ingest table | (no gap; P4 sampling bias) |
| `src/txn/txn_timestamp.c:548` | FIXME-WT-16310: synchronization around oldest_timestamp and stable_timestamp | TT-GC1 (related) |

---

*End of analysis.*
