# Gap Analysis: DisAgg Stories/Epics + Remaining DisAgg Tickets

Generated: 2026-05-06

---

## Section A: Stories and Epics (Group A — 25 tickets)

These are the 25 DisAgg Stories and Epics from the project backlog. Each was queried from Jira
for its description, acceptance criteria, and sub-tasks. Testing implications are assessed against
the existing 110+ gaps already documented in `test/analysis/05_scenario_analysis/00_synthesis.md`.

---

### [WT-14408] [ds-04.02] — Pre-mortems on durability (data corruption)

**Testing requirements implied:** Durability scenarios covering data corruption modes: partial
writes, torn checkpoints, incomplete delta flushes, SLS partial-write scenarios, and power-loss
scenarios. Requires simulation of SLS write failures mid-checkpoint.

**Currently covered?** Partial — basic crash recovery tests exist; SLS-level fault injection does
not exist at WT level.

**Gaps identified:**
- No WT-level test injecting SLS write failures mid-checkpoint to validate durability invariants.
- No systematic enumeration of data corruption modes from the pre-mortem analysis feeding into WT tests.
- Failpoint before checkpoint completion (see WT-16901) is planned but not done.

---

### [WT-14413] [ds-05.08] — Restore with RTO < 15 mins

**Testing requirements implied:** WT startup/recovery time must be observable and bounded.
Restore time metrics need to be exposed from WT. At 250k tables, checkpoint pickup already
exceeds 27 minutes (WT-17352).

**Currently covered?** No — no WT-side metrics or tests for restore duration. Performance
tests for checkpoint pickup exist conceptually but not as automated regression tests.

**Gaps identified:**
- No test measuring WT checkpoint pickup time as a function of table count.
- No WT metric or statistic tracking "time to first write after step-up."
- Checkpoint pickup performance at scale (WT-17352) is an open epic; 27+ min at 250k tables
  violates the 15-min RTO.

---

### [WT-14415] [ds-06.08] — Change stream support

**Testing requirements implied:** Timestamped fast truncate on layered tables; the change stream
pre-image retention depends on `WT_CURSOR::truncate` with timestamps on follower nodes.
Sub-task SPM-4375 tracks the WT work. Out-of-order timestamp handling during recovery is
separately tracked (WT-16663).

**Currently covered?** No WT-level tests for timestamped fast truncate on layered tables.
`test_layered_fast_truncate01-03` exercise the unsupported code path and are expected to be
updated once the feature lands.

**Gaps identified:**
- No test for timestamped fast truncate on a layered table (leader or follower).
- `change_stream_pre_image_startup_recovery.js` is excluded from disagg override suites (WT-16663).
- Fast truncate is currently DEFERRED; once enabled, these tests need to be created.

---

### [WT-14416] [ds-06.09] — Existing functional tests pass when compared to local storage clusters

**Testing requirements implied:** Test suites that currently pass on local storage should pass
(or have documented known failures) when run in disagg mode.

**Currently covered?** Partially — a disagg override suite exists; however several test
categories are explicitly excluded (transactions, fast truncate, backup, modify, etc.).

**Gaps identified:**
- Multi-document transaction tests are entirely excluded from disagg override.
- Tests using `checkpoint=WiredTigerCheckpoint` on cursors fail — layered checkpoint cursors
  not yet supported (WT-15357).
- Tests using `WT_CURSOR::modify()` excluded (WT-16978).
- Quantifying "percentage of WT tests passing in disagg mode" requires a tracking mechanism.

---

### [WT-14420] [ds-07.04] — MongoD stores intermediate key (KEK) for decrypting data encryption keys in SLS

**Testing requirements implied:** WT must support storing KEK in SLS (shared log service) and
retrieving it on startup. Key lifecycle: store, retrieve, rotate. WT must not expose plaintext
keys in logs or statistics.

**Currently covered?** No — encryption integration with DisAgg has no WT-level tests.

**Gaps identified:**
- No test for KEK lifecycle in disagg mode (store on leader, retrieve on follower).
- No test for key rotation in disagg mode.
- No test verifying KEK is not exposed in WT logs/statistics.

---

### [WT-14423] [ds-08.06] — Local development environment for mongod/wt for unified binary

**Testing requirements implied:** Infrastructure story; no direct WT testing requirements.
Relates to ensuring WT tests can run locally against a PALITE stand-in.

**Currently covered?** PALITE exists for local testing. No new test gaps from this story.

**Gaps identified:** None directly applicable to WT test suite.

---

### [WT-14427] [ds-09.04] — 100% hygiene plan execution

**Testing requirements implied:** Code hygiene — removing dead code, FIXMEs, asserts replaced
by proper error handling. Ticket response was too large to fully analyze.

**Currently covered?** Code quality, not directly testable. FIXME/TODO search in the codebase
is covered by the non-disagg analysis agent.

**Gaps identified:** Hygiene items that disable functionality (e.g., assert-guarded features)
may block test coverage until the hygiene item is resolved. Track via WT-14427 sub-tasks.

---

### [WT-14429] [ds-12.01] — Automated development environments

**Testing requirements implied:** Infrastructure story (CI/CD environment automation). No direct
WT unit/integration test requirements.

**Currently covered?** N/A — infrastructure, not a test coverage gap.

**Gaps identified:** None directly applicable to the WT test suite.

---

### [WT-14432] [ds-14.03] — Performance benchmarking of hardware/pod specs within Atlas

**Testing requirements implied:** Performance measurement infrastructure. WT needs to expose
statistics relevant for Atlas sizing decisions.

**Currently covered?** Partial — WT statistics exist; Atlas-specific benchmarks are not WT
responsibility.

**Gaps identified:** No automated performance regression test in WT CI for disagg workloads
(see WT-14435 / ds-14.06 below). Block manager stats (bytes_total accounting) bugs (WT-16660,
WT-17034) may distort measurements.

---

### [WT-14433] [ds-14.04] — Read performance matching NVME via a local cache

**Testing requirements implied:** Local cache layer (Monarch) integration; WT block manager
must serve reads from local cache before hitting SLS. Cache hit rate statistics required.

**Currently covered?** Post-GA item. No current WT test coverage for cache-assisted reads.

**Gaps identified:** Post-GA; defer until local cache design is finalized.

---

### [WT-14434] [ds-14.05] — Achieve performance parity with latest MongoD

**Testing requirements implied:** Comparative benchmarks. Requires a stable benchmark suite
running disagg and non-disagg WT heads-up.

**Currently covered?** No automated disagg-vs-local performance regression test.

**Gaps identified:**
- Eviction walk inefficiency on ingest btrees (WT-17173) degrades read performance on followers.
- No test that validates follower read latency against a baseline after checkpoint pickup.

---

### [WT-14435] [ds-14.06] — Automated Performance Regression Tests

**Testing requirements implied:** Evergreen-integrated performance tests that catch disagg
regressions automatically (throughput, latency, cache efficiency).

**Currently covered?** No — existing perf tests are not disagg-specific.

**Gaps identified:**
- No disagg-specific performance regression test in Evergreen CI.
- Checkpoint pickup time regression (WT-17352) has no automated detection.

---

### [WT-14436] [ds-14.07] — High Value Workload (non-YCSB) performance testing

**Testing requirements implied:** Domain-specific workload performance tests (e.g., time-series,
geo-spatial, aggregation pipelines) in disagg mode.

**Currently covered?** No disagg-specific workload performance tests.

**Gaps identified:** WT-level hook: `WT_CURSOR::modify()` must be enabled in disagg for YCSB
update-heavy workloads (WT-16978 rolled back).

---

### [WT-14440] [ds-19.01] — Automatic recovery testing for process, HW, or networking failures

**Testing requirements implied:** Chaos/gameday testing framework. WT must survive:
- Process kill mid-checkpoint
- Network partition between WT and SLS
- SLS node failure with partial writes
- Hardware failure during log flush

**Currently covered?** No automated chaos tests exist for disagg. Gameday testing is manual.

**Gaps identified:**
- No automated test for disagg process kill mid-checkpoint (failpoint needed, see WT-16901).
- No test for SLS network partition simulation (PALITE can simulate via error injection).
- No WT-level recovery correctness test after SLS node failure.

---

### [WT-14441] [ds-21.01] — Complete Durability threat model of SLS with mongod

**Testing requirements implied:** Formal threat model; once completed, new threat scenarios
must have corresponding tests.

**Currently covered?** Threat model not yet complete. Durability threat model gaps may surface
additional WT testing requirements.

**Gaps identified:** Placeholder — track sub-tasks of WT-14441 as they are defined to identify
concrete WT test gaps.

---

### [WT-14442] [ds-28.02] — Mongod admission control using SLS metrics

**Testing requirements implied:** WT must expose an interface for mongod to apply backpressure
based on SLS load metrics. WT-side: either a callback registration or statistics polling
interface; mongod-side: rate limiting when SLS reports stress.

**Currently covered?** No WT-level tests for the admission control interface.

**Gaps identified:**
- No test verifying WT exposes SLS stress metrics to mongod.
- No test verifying backpressure callback/interface from WT to mongod layer.
- Interface design (callback vs. polling) not finalized in ticket.

---

### [WT-14454] [ds-04.03] — Pre-mortems on availability

**Testing requirements implied:** Availability scenarios: leader failure, follower lagging,
no available leader, split-brain prevention.

**Currently covered?** Limited — role transition tests exist but no dedicated availability
pre-mortem test scenarios.

**Gaps identified:**
- No test for "leader fails, follower must step up within bounded time" scenario.
- No test for "all nodes are followers" (no leader) graceful degradation.

---

### [WT-14463] [ds-19.06] — Complete Availability threat model of SLS with mongod

**Testing requirements implied:** Jira description was template-only placeholder; no concrete
scope defined yet. Once threat model is written, WT tests must cover identified scenarios.

**Currently covered?** N/A — threat model not written.

**Gaps identified:** Placeholder; revisit once availability threat model scope is defined.

---

### [WT-14491] — Coordinate table drops across secondaries

**Testing requirements implied:** Jira acceptance criteria explicitly state:
1. After dropping shared table, loading the last checkpoint should result in the table being
   inaccessible on followers.
2. Primary should drop tables from shared metadata (not just local metadata).

**Currently covered?** No dedicated test for coordinated table drop across leader + follower nodes.

**Gaps identified:**
- No test: drop table on leader → follower picks up checkpoint → table must be inaccessible.
- No test: verify dropped table is removed from shared metadata (not just local).
- Both acceptance criteria are directly testable at WT level using PALITE.

---

### [WT-14664] [ds-09.05] — Design Review + Document for Layered Tables

**Testing requirements implied:** Documentation and design review story. Design decisions
surface testing requirements (e.g., checkpoint ordering, GC constraints).

**Currently covered?** Documentation; no direct test gaps.

**Gaps identified:** Design decisions already implemented; any gaps from design doc are
captured by other tickets. No new gaps from this story itself.

---

### [WT-14906] [ds-06.05] — Multi-document transactions

**Testing requirements implied:** Distributed commit protocol; two-phase commit across WT +
SLS; transaction coordinator failure handling; MDT isolation in disagg mode. 38-week
implementation estimate. Target: Public Preview.

**Currently covered?** No MDT tests in disagg mode. Multi-document transactions are entirely
disabled/excluded from disagg test suites.

**Gaps identified:**
- No tests for MDT prepare → commit lifecycle in disagg mode.
- No tests for MDT coordinator failure recovery in disagg mode.
- No tests for MDT isolation (snapshot isolation) in presence of disagg checkpoints.
- All deferred until MDT implementation (WT-14906) is ready.

---

### [WT-15476] — Validate layered table content during garbage collection (Epic)

**Testing requirements implied:** GC validation — when ingest records are pruned, the
corresponding data must exist in the shared checkpoint. Diagnostic mode should verify this
invariant. Sub-tasks are actively in progress (target: 2026-05-15).

**Currently covered?** Partial — GC exists; validation of pruned records against shared
checkpoint is the open work.

**Gaps identified:**
- No test in "GC diagnostic mode" verifying pruned ingest records exist in shared checkpoint.
- No test for GC validation when shared checkpoint is corrupt (negative test).
- Active development in progress; tests should be added alongside implementation.

---

### [WT-16720] — Validation improvements in disagg (Epic)

**Testing requirements implied:** Improve `wiredtiger_open` verify and `WT_SESSION::verify`
to work correctly in disagg mode; validate layered table invariants (ingest vs. shared
content consistency); verify delta chain correctness.

**Currently covered?** Partial — `verify()` exists but disagg-specific invariants may not
be checked. SO-H4 (verify on follower) is an existing HIGH-priority gap.

**Gaps identified:**
- No test verifying disagg-specific invariants (delta chain integrity, ingest/shared
  content consistency) via `WT_SESSION::verify()`.
- No test for `verify()` on follower after checkpoint pickup.
- Sub-tasks of WT-16720 should each have a corresponding test when implemented.

---

### [WT-17105] — Disagg Bugs (Epic)

**Testing requirements implied:** Bug tracker epic. Each bug in this epic implies a missing
regression test. Bugs without reproduction tests are future regression risks.

**Currently covered?** Each sub-bug needs a targeted regression test; this varies per bug.

**Gaps identified:**
- Any bug fixed without a corresponding regression test is a potential gap.
- Specific bugs with no tests identified: WT-17300 (ENOENT propagation), WT-17253 (TSAN
  data race), WT-16660 (bytes_total accounting), WT-17034 (addr_pack failure path).

---

### [WT-17352] — WT (Disaggregated Storage) Checkpoint Pickup Performance (Epic)

**Testing requirements implied:** Checkpoint pickup time must scale gracefully with table count.
Currently 27+ min at 250k tables due to eager ingest table creation. Lazy creation is the
planned fix. Acceptance criterion: < 15 min for 250k tables (to meet RTO SLA from WT-14413).

**Currently covered?** No automated test measuring checkpoint pickup time vs. table count.

**Gaps identified:**
- No test: measure checkpoint pickup time as a function of number of layered tables.
- No test: verify lazy ingest table creation skips unused tables during pickup.
- No performance regression detection for checkpoint pickup time in CI.

---

## Section B: Remaining DisAgg Tickets (Group B — triaged subset)

These are selected tickets from the 162-ticket Group B that showed the highest testing gap
potential. Infrastructure/build/perf-regression tickets without WT test implications were
skipped.

---

### Confirmed Testing Gaps

---

#### [WT-14491] — Coordinate table drops across secondaries

**Type/Priority/Status:** Story / Major-P3 / Backlog

**Jira context:** Acceptance criteria state (1) dropped table must be inaccessible on follower
after checkpoint pickup, and (2) shared metadata must be cleaned up (not just local).

**Gap:** No test for table drop coordination across leader and follower in PALITE.

**Suggested test:** Drop a layered table on leader → leader checkpoints → follower picks up
checkpoint → assert follower cursor on dropped table returns `WT_NOTFOUND`. Separately verify
shared metadata does not list the table.

**Already in existing analysis?** No.

---

#### [WT-15357] — Layered checkpoint cursors

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** `checkpoint=WiredTigerCheckpoint` is not supported on layered cursors.
Many python tests rely on this. The fix should be straightforward — reuse existing stable
cursor code for both leader and follower.

**Gap:** No test for `wt_session.open_cursor(uri, config="checkpoint=WiredTigerCheckpoint")`
on a layered table. Many existing tests implicitly require this.

**Suggested test:** Open a layered table cursor with `checkpoint=WiredTigerCheckpoint` on both
leader and follower; verify reads return the expected snapshot data; test after role transition.

**Already in existing analysis?** No.

---

#### [WT-15476] — Validate layered table content during garbage collection (Epic)

**Type/Priority/Status:** Epic / Major-P3 / Open

**Jira context:** GC diagnostic mode should verify that pruned ingest records exist in the
corresponding shared checkpoint. Active development, target 2026-05-15.

**Gap:** No test exercising GC validation mode; no negative test for corrupt shared checkpoint.

**Suggested test:** Run GC with diagnostic mode enabled on a layered table; inject a missing
shared checkpoint entry; assert validation detects the discrepancy and returns an error.

**Already in existing analysis?** Mentioned in synthesis but no concrete test scenario defined.

---

#### [WT-15594] — Timestamp enforcement on layered table writes

**Type/Priority/Status:** Task / Major-P3 / Open (WT-14520 tracker)

**Jira context:** All writes to layered tables must use timestamps. Asserts are not yet added
to enforce this. Without enforcement, step-up correctness cannot be guaranteed.

**Gap:** No test verifying that a non-timestamped write to a layered table is rejected with
an appropriate error/assert.

**Suggested test:** Attempt to insert/update a layered table record without a commit timestamp;
assert `EINVAL` or similar error is returned.

**Already in existing analysis?** No.

---

#### [WT-16044] — Duplicate phylog entries under cache pressure

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Under cache pressure, split pages can generate duplicate phylog entries.
Partially fixed (WT-16244) but full fix requires delta building for split pages.

**Gap:** No stress test that triggers page splits under cache pressure and validates phylog
entry uniqueness.

**Suggested test:** Run a high-write-throughput workload on a memory-constrained layered table
(small cache) to trigger splits; validate phylog for duplicate entries using existing
diagnostic tooling.

**Already in existing analysis?** No.

---

#### [WT-16452] — WT_CURSOR::modify() in disagg (WT-16978 context)

**Type/Priority/Status:** Bug/Task / Major-P3

**Jira context:** `modify()` was disabled (rolled back) after causing assertion failures in
disagg mode. Must be re-enabled after test/format validation and YCSB workload testing.

**Gap:** `modify()` is currently disabled in disagg; no test validates its correctness once
re-enabled. test/format must be updated to exercise `modify()` in disagg mode.

**Suggested test:** Enable `modify()` in disagg (when WT-16978 is resolved); run test/format
with `modify` operation enabled on layered tables; validate read-back correctness and absence
of assertion failures.

**Already in existing analysis?** Partially (modify is in the unsupported list but no test is
specified for the re-enablement path).

---

#### [WT-16494] — Checkpoint order monotonicity across role changes

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Non-atomic local + remote metadata updates create a window where checkpoint
order can be violated across step-up/step-down. A node that steps up might see an older
checkpoint than expected.

**Gap:** No targeted test for checkpoint order monotonicity after step-up (follower sees leader
checkpoint N, steps up, checkpoint N+1 must be strictly newer than N).

**Suggested test:** Run leader (checkpoint=N) → follower picks up N → step-up → verify new
checkpoint number N+1 > N. Repeat with concurrent writers to increase race likelihood.

**Already in existing analysis?** No.

---

#### [WT-16532] — Eviction walk inefficiency on ingest btrees (WT-17173 context)

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Eviction walks are wasteful between checkpoint pickups on followers because
most ingest pages have a prune_timestamp newer than what can be evicted. Application threads
stall waiting for eviction progress.

**Gap:** No test validates eviction behavior on a follower when prune_timestamp is stale
(i.e., follower has not picked up a new checkpoint recently).

**Suggested test:** Create a follower, write data to leader, stall checkpoint pickup on follower,
verify eviction does not stall application threads (or stalls gracefully within bounds).

**Already in existing analysis?** No.

---

#### [WT-16627] — Out-of-order timestamp in history store during change stream recovery

**Type/Priority/Status:** Bug / Major-P3 / Open (WT-16663 context)

**Jira context:** `change_stream_pre_image_startup_recovery.js` is excluded from
`no_passthrough_disagg_override` suite because the test triggers out-of-order timestamp
handling not yet supported in disagg.

**Gap:** Test is excluded from disagg suite; no disagg-specific recovery test for out-of-order
timestamp in history store.

**Suggested test:** Once WT-16663/WT-16627 are resolved, re-enable
`change_stream_pre_image_startup_recovery.js` in disagg mode and add a WT-level
regression test for out-of-order timestamp during recovery.

**Already in existing analysis?** No.

---

#### [WT-16660] / [WT-17034] — bytes_total accounting bug (addr_pack failure)

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Storage accounting leak when `addr_pack` fails mid-write. `bytes_total`
for the disagg block manager is not decremented on failure path. No test for `addr_pack`
failure path.

**Gap:** No test for the `addr_pack` failure path and subsequent `bytes_total` accounting.

**Suggested test:** Use a failpoint to inject an `addr_pack` failure during a checkpoint write;
verify `bytes_total` statistic is unchanged after the failed write (no leak).

**Already in existing analysis?** No.

---

#### [WT-16901] — Failpoint before checkpoint completion

**Type/Priority/Status:** Task / Major-P3 / Open

**Jira context:** Planned failpoint to inject a failure before checkpoint completion to validate
rollback correctness. Not yet implemented.

**Gap:** No test for partial checkpoint rollback in disagg mode.

**Suggested test:** Inject failpoint mid-checkpoint; verify that the incomplete checkpoint is
not visible to followers; verify leader can complete a subsequent full checkpoint successfully.

**Already in existing analysis?** Mentioned in GC/durability context; no concrete test scenario.

---

#### [WT-17173] — Ingest table prune-timestamp-aware eviction

**Type/Priority/Status:** Bug/Task / Major-P3 / Open

**Jira context:** See WT-16532 above. Eviction ignores prune_timestamp, leading to wasted
eviction walks and application stalls on followers.

**Gap:** No test for eviction efficiency on follower with stale prune_timestamp.

**Suggested test:** As described under WT-16532.

**Already in existing analysis?** No.

---

#### [WT-17253] — Shutdown/sweep TSAN data race

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Sweep server reads sessions that prefetch teardown has already zeroed,
causing a TSAN data race on shutdown.

**Gap:** No targeted test for concurrent sweep + shutdown in disagg mode with TSAN enabled.

**Suggested test:** Under TSAN build, run repeated open/close of layered table connections with
sweep enabled; assert no data-race detected during shutdown sequence.

**Already in existing analysis?** No.

---

#### [WT-17300] — `__curstat_size_only` ENOENT propagation

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** TOCTOU race in statistics cursor fast path; `ENOENT` (file deleted between
stat check and open) is not properly falling back to slow path, leaving statistics incorrect.

**Gap:** No test for statistics cursor behavior when underlying layered table file is removed
between the fast-path stat check and open.

**Suggested test:** Open a statistics cursor on a layered table while concurrently dropping
the table; verify no crash and that statistics return `ENOENT` or zero values gracefully.

**Already in existing analysis?** No.

---

#### [WT-17323] — Sweep server and layered table dhandle sweeping

**Type/Priority/Status:** Bug / Major-P3 / Open

**Jira context:** Layered table dhandle sweeping is explicitly skipped in `conn_sweep.c`.
At 50k collections, FD exhaustion crashes are seen. No test for FD exhaustion under high
table count.

**Gap:** No test for FD exhaustion under high table count with sweep disabled for layered tables.

**Suggested test:** Open 50k+ layered tables (or simulate via config); verify no FD exhaustion
crash occurs; verify that once sweep is re-enabled for layered tables, FDs are released
between checkpoint pickups.

**Already in existing analysis?** No.

---

#### [WT-17345] / [WT-17346] — wt util disagg mode argument validation

**Type/Priority/Status:** Task / Major-P3 / Open

**Jira context:** `wt util` subcommands and flags that are unsupported in disagg mode should
return a clear error rather than silently misbehaving or crashing. WT-17346 is the companion
ticket. Etienne Petrel noted: needs a list of specific subcommands/flags to disallow and
expected error behavior.

**Gap:** No test verifying that unsupported `wt util` subcommands return a clean error in
disagg mode.

**Suggested test:** For each disallowed subcommand (e.g., `backup`, `salvage`, `compact`),
run `wt -C "disagg_storage=true" <subcommand>`; verify `EINVAL` or similar with a clear
error message.

**Already in existing analysis?** No.

---

#### [WT-17352] — Checkpoint Pickup Performance (Epic)

**Type/Priority/Status:** Epic / Major-P3 / Open

**Jira context:** 27+ minutes at 250k tables due to eager ingest table creation during pickup.
Fix: lazy creation. This also impacts RTO SLA (WT-14413 requires < 15 min restore).

**Gap:** No automated test measuring checkpoint pickup time vs. table count.

**Suggested test:** Create N layered tables (N = 1k, 10k, 100k), write one checkpoint,
time the follower `checkpoint_meta` reconfigure; assert time is below a threshold.
Run in a Python test with multiple N values to characterize the scaling curve.

**Already in existing analysis?** No.

---

#### [WT-14415] — Change stream support (fast truncate for layered tables)

**Type/Priority/Status:** Story / Major-P3 / Open

**Jira context:** Fast truncate on layered tables is needed for change stream pre-image
expiry (timestamped fast truncate). Currently DEFERRED. SPM-4375 tracks the WT implementation.

**Gap:** No test for timestamped fast truncate on a layered table (leader or follower).

**Suggested test:** Once SPM-4375 lands: write records with timestamps → timestamped truncate
to remove pre-images up to a timestamp → verify records before truncation point are gone →
verify records after truncation point remain.

**Already in existing analysis?** No.

---

### Uncertain Cases

---

#### [WT-14906] — Multi-document transactions in disagg

**Type/Priority/Status:** Story / Major-P3 / Open (38-week estimate)

**Jira context:** Requires distributed commit protocol in WT + SLS. Design not complete.

**Gap uncertainty:** MDT in disagg is a large feature; cannot define test scenarios without
design. Once WT-14906 sub-tasks are defined, they will generate concrete test requirements.

**Recommendation:** Placeholder — revisit when MDT design is complete (target: Public Preview).

---

#### [WT-14440] — Automatic recovery testing for process/HW/network failures

**Type/Priority/Status:** Story / Major-P3 / Open

**Jira context:** Chaos/gameday testing framework at Atlas level; WT contribution is ensuring
WT handles failure modes gracefully.

**Gap uncertainty:** WT-level fault injection (PALITE error injection) can cover SLS failures;
Atlas-level chaos (pod kills, network partitions) are outside WT CI. Unclear which failure
modes WT owns directly vs. Atlas SRE.

**Recommendation:** Define clear WT-level failure modes (e.g., "SLS write returns error during
checkpoint") and add failpoint-based tests for those. Escalate Atlas-level chaos to Atlas team.

---

#### [WT-14442] — Mongod admission control using SLS metrics

**Type/Priority/Status:** Story / Major-P3 / Open

**Jira context:** WT must expose SLS stress metrics; mongod will use them for rate limiting.
Interface not yet defined.

**Gap uncertainty:** Depends on interface design (callback vs. statistics polling). Once
the interface is defined, test scenarios become clear.

**Recommendation:** Add test for statistics-based SLS load reporting once interface is defined.

---

#### [WT-15540] — Disagg eviction / cache interactions

**Type/Priority/Status:** Task / Major-P3

**Jira context:** Covers eviction behavior in disagg — interaction with prune_timestamp,
ingest vs. shared pages. Overlaps with WT-17173 and WT-16532.

**Gap uncertainty:** Some sub-scenarios may already be covered by eviction stress tests.

**Recommendation:** Verify that eviction tests run with disagg configuration; add follower-
specific eviction stress test (high write rate, small cache, stale prune_timestamp).

---

#### [WT-16002] — Materialization frontier behavior

**Type/Priority/Status:** Bug/Task / Major-P3

**Jira context:** Materialization frontier controls what followers can read from shared storage.
Incorrect frontier can cause stale reads or crashes.

**Gap uncertainty:** Frontier management may already be covered by existing follower read tests.
Unclear if frontier boundary conditions (at exactly the frontier, just past it) are tested.

**Recommendation:** Add test for follower reads at exactly the materialization frontier (should
return the newest visible version, not newer uncommitted data).

---

### No Gap (brief)

The following tickets from Group B were examined but found to have no direct WT test gaps,
either because they are infrastructure/build-only, already covered, or the feature is deferred
with no testable WT interface yet:

- **WT-14423** [ds-08.06] — Local dev environment (infra, not test coverage)
- **WT-14429** [ds-12.01] — Automated dev environments (infra)
- **WT-14408** [ds-04.02] — Pre-mortems on durability (threat model work; tests follow once threat model written)
- **WT-14432** [ds-14.03] — Hardware benchmarking within Atlas (Atlas-level, not WT tests)
- **WT-14433** [ds-14.04] — Read perf / NVME local cache (Post-GA, design not finalized)
- **WT-14664** [ds-09.05] — Design review for layered tables (documentation/design)
- **WT-14454** [ds-04.03] — Pre-mortems on availability (threat model work)
- **WT-14463** [ds-19.06] — Availability threat model (template placeholder; no scope yet)
- **WT-14441** [ds-21.01] — Durability threat model (in-progress; tests follow from sub-tasks)
- **WT-16720** validation improvements (Epic tracker; sub-tasks drive individual tests)
- **WT-17105** disagg bugs (Epic tracker; individual bugs listed separately above)
- **WT-14476** / **WT-14470** / **WT-14469** — Build/tooling improvements (no WT test impact)
- **WT-15594** timestamp enforcement (now tracked under WT-14520 in Group A context)

---

## Major New Testing Areas Discovered

These are the highest-impact testing gaps identified in this analysis that are NOT covered
by the existing 110+ gap list in `test/analysis/05_scenario_analysis/00_synthesis.md`:

### 1. Table Drop Coordination Across Nodes (WT-14491)
Acceptance criteria are directly testable at WT level using PALITE. Two test scenarios:
- Drop → checkpoint → follower pickup → verify inaccessible
- Drop → verify shared metadata cleanup

### 2. Layered Checkpoint Cursors (WT-15357)
`checkpoint=WiredTigerCheckpoint` on layered cursors is unimplemented but widely required
by python tests. Straightforward fix with clear test path.

### 3. Checkpoint Pickup Performance at Scale (WT-17352)
Directly violates the 15-min RTO SLA at 250k tables (WT-14413). No automated regression
test exists. Should be added to CI as a performance gate.

### 4. wt util Argument Validation in Disagg Mode (WT-17345/17346)
Unsupported subcommands should return clean errors. No enforcement or tests exist.
Directly testable with a parameterized test over the list of disallowed subcommands.

### 5. Timestamp Enforcement on Layered Table Writes (WT-14520/WT-15594)
Asserts not yet added. Without enforcement, step-up correctness is at risk. Test:
non-timestamped write to layered table should return error.

### 6. FD Exhaustion Under High Table Count (WT-17323)
Sweep skips layered dhandles. At 50k collections, crashes occur. Test: open 50k layered
tables and verify no FD exhaustion. Needed for disagg scalability claims.

### 7. Checkpoint Order Monotonicity After Role Changes (WT-16494)
Non-atomic metadata updates create a correctness window. No test for monotonicity invariant
across step-up. Medium complexity, high correctness value.

### 8. GC Validation — Ingest vs. Shared Table Content (WT-15476)
Active development (target 2026-05-15). Diagnostic mode test must be added alongside
implementation. This is the most directly actionable near-term gap.

### 9. Failpoint Mid-Checkpoint for Durability Validation (WT-16901)
Planned but not done. Test that validates rollback correctness when checkpoint fails
partway through. Required for durability pre-mortem (WT-14408).

### 10. Statistics Cursor ENOENT Race (WT-17300) + bytes_total Accounting (WT-16660/17034)
Two separate data integrity bugs in the statistics/accounting path, neither has a regression
test. Both should be straightforward to add with existing failpoint infrastructure.

---

*Analysis by: Ivan Kochin, 2026-05-06*
*Source tickets: Group A (25 Stories/Epics from agent_g7.txt), Group B (triaged from agent_g8.txt)*
*Reference: test/analysis/05_scenario_analysis/00_synthesis.md (110+ existing gaps, not repeated here)*
