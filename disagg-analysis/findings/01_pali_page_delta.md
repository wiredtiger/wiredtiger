# Gap Analysis: PALI / Page Deltas / PAGE_LOG / Disagg Block Manager
Generated: 2026-05-06

Source of tickets: `/tmp/agent_g1.txt` (28 tickets, WT-14504 through WT-17341)

---

## Confirmed Testing Gaps

### [WT-14555] — Review and correct how page deltas are accounted for in read/write stats
**Type/Priority/Status:** Bug / Major-P3 / Open (actively in sprint as of 2026-05-04)
**Jira context:** The ticket documents that `block_byte_read`, `block_byte_write`,
`disagg_block_hs_byte_read`, and related stats are incorrectly accounted for when
page deltas are in play. In the read path, byte stats were recorded before the
actual data was returned from PALI (so the per-delta sizes of the full result array
were not summed). A PR-in-review diff was quoted showing the required fix. Three
sub-tickets (WT-14469, WT-14470, WT-14546) are also linked.
**Gap:** No test asserts specific byte-level statistics (`block_byte_read`,
`block_byte_write`, `disagg_block_hs_byte_read`) for scenarios that produce page
deltas. Code search confirms zero existing tests assert these stats in any disagg
test. The existing `test_leaf_delta_disagg01.py` and `test_layered63.py` assert
delta *count* stats but never byte amounts.
**Suggested test:** A test that writes a known payload (measurable size), generates
a delta, reads the page back, and then asserts `block_byte_read` equals
`full_page_size + delta_size` (not just `full_page_size`). Should cover both
history-store (`disagg_block_hs_byte_read`) and non-HS paths, and compare the
delta path result against the classic block manager's comparable stat.
**Already in existing analysis?** No — NEW GAP

---

### [WT-14504] — Avoid writing duplicate full page to PALI
**Type/Priority/Status:** Task / Major-P3 / Open (unassigned)
**Jira context:** Reconciliation can produce an identical page to the previously
written one (no new changes), but the current code does not detect this and writes a
redundant full-page to PALI. The ticket is distinct from delta skipping: even when
delta is disabled or ineligible (e.g. delta_count at max), a full-page identical to
the prior one may be re-sent.
**Gap:** No test verifies that redundant-full-page writes are suppressed. The
`rec_skip_write` stat exists (used by `WT_MULTI_SKIP_WRITE`), but no test checks it
specifically in the identical-page scenario. The code in `rec_write.c` around the
`skip_write` path covers some cases (no newer updates, single-page result, not an
in-memory split), but the deduplication check is structural, not equality-based,
so a silent regression would not be caught.
**Suggested test:** Populate a page, checkpoint, reopen, make a read-only
"touch" (no actual update), checkpoint again, and assert `rec_skip_write` incremented
and that the page_log put count (`disagg_block_put`) did NOT increase vs. baseline.
**Already in existing analysis?** No — NEW GAP

---

### [WT-15940] — wt util fails with palite error when using a disagg config with non-disagg db
**Type/Priority/Status:** Bug / Major-P3 / Backlog
**Jira context:** Running `wt -C 'disaggregated=(role=leader,page_log=palite)'`
against a database that was NOT created with disagg config triggers a misleading
`unknown page log 'palite'` error because the PALite extension library is not
automatically loaded when the page-log name is provided without the extension path.
The DoD explicitly asks for a `self.runWt(...)` test to cover this.
**Gap:** No test uses `self.runWt` or the `wt` command-line tool against a non-disagg
database with a disagg config string. The wt-util test suite (`test_wt*.py`) has no
disagg-specific cases. Correct behavior (graceful warning, not crash/confusing error)
is entirely untested.
**Suggested test:** Use `self.runWt` to run `wt -C '<disagg-config>' -h <non-disagg-dir> verify`,
assert the error output contains a recognisable, user-friendly message such as
"incompatible database configuration" rather than the raw `unknown page log` internal
error. Also test the inverse: providing the full extension path resolves the issue.
**Already in existing analysis?** No — NEW GAP

---

### [WT-16442] — Write Performance Reconciliation Efficiency - Delta Generation for re-split pages
**Type/Priority/Status:** Task / Major-P3 / Open
**Jira context:** Pages that were split during a previous reconciliation cannot
currently have deltas generated on re-reconciliation because reconciliation cannot
produce the same split boundaries as before. The code in `src/reconcile/rec_write.c`
at line 3332 has an explicit `/* FIXME-WT-15709: build delta for split pages. */`
comment, and the condition `r->multi_next == 1` used to gate delta generation means
any re-split page falls through to a full-page write. This is a correctness/coverage
gap: such pages always write full pages silently.
**Gap:** No test verifies that a page which was previously split and then re-split
generates — or explicitly does NOT generate — a delta, nor that the stat
`rec_page_delta_rejected_*` accounts for this case. `test_layered56.py` tests the
split-vs-no-split delta outcome but only for the initial split, not for pages split
a second time. There is no test that exercises the future feature path described in
the FIXME.
**Suggested test:** Force a page to split (checkpoint), then update it so it splits
again on the next checkpoint. Assert `rec_page_delta_leaf == 0` (confirming the
current restriction) and `rec_page_delta_rejected_*` or the absence of delta stat
increment, so the restriction is contractually tested. Once WT-15709/WT-16442 are
implemented, the assertion can be inverted.
**Already in existing analysis?** No — NEW GAP

---

### [WT-16239] — Write a full page instead of delta if we have a lot of deletes on the page become globally visible
**Type/Priority/Status:** Improvement / Major-P3 / Open (future sprint)
**Jira context:** Writing a delete-record delta consumes more disk space than a full
page that omits the deleted entries. The improvement adds a heuristic: if a page has
many globally-visible tombstones, write a full page to reclaim space. This is a
space-amplification correctness scenario.
**Gap:** No test verifies that `rec_page_delta_leaf == 0` (full page chosen) when a
configurable threshold of globally-visible deletes exists on a page, nor that disk
space is actually reclaimed. The existing delete delta test in `test_leaf_delta_disagg01.py`
tests that a delta IS written on delete, not that the system switches back to a full
page when it should.
**Suggested test:** Populate a page, delete the majority of its rows (past a
threshold), advance the stable timestamp to make them globally visible, checkpoint,
and assert a full page was written (`rec_page_delta_leaf == 0` for that page) and that
the PALI page_log response after the next checkpoint does not return stale delta
tombstones.
**Already in existing analysis?** No — NEW GAP

---

### [WT-16535] — Ensure WT_PAGE_LOG_ENCRYPTED is default set for regular tables
**Type/Priority/Status:** Task / Major-P3 / Open (assigned to Jie Chen)
**Jira context:** PALI auto-encrypts all data passing through `pl_handle_put`/`pl_handle_get`.
This is problematic for internal WiredTiger tables (shared turtle file, encryption key
provider table) which must bypass encryption to allow startup without the original KEK.
`WT_PAGE_LOG_ENCRYPTED` flag controls per-call encryption. The fix requires ensuring
regular user tables set this flag by default, while internal tables clear it. Dependent
on SERVER-117943.
**Gap:** No test verifies that: (a) regular table pages are written to PALI with the
`WT_PAGE_LOG_ENCRYPTED` flag set, (b) the encryption key provider and turtle-file
tables bypass encryption (flag NOT set). The `test_key_provider_disagg01/02.py`
tests cover key provider behavior but do not check the encryption flag path through
PALI. A regression (wrong flag on key provider) would cause silent silent startup
failures, not caught by any current test.
**Suggested test:** In a build with PALI (not PALite) and encryption enabled, create
a regular table and a key-provider table. Intercept or mock the `pl_handle_put` calls
(or use a PALite instrumentation hook) to assert flag presence. Alternatively, use a
diagnostic log message check to confirm the encrypted/non-encrypted path is taken
for each table type.
**Already in existing analysis?** No — NEW GAP

---

### [WT-15684] — Make PALI implementation configurable in test/model
**Type/Priority/Status:** Task / Major-P3 / Open
**Jira context:** `test/model` tests hardcode `page_log=palm` in the connection
config. The DoD requires: page_log setting configurable from external files or
command-line, implementation selectable without source changes, and an Evergreen task
that can select page_log=palite or page_log=pali.
**Gap:** Until this is done, model tests only ever run against PALM (or its removal
successor). Any PALI-specific behavior differences (e.g., real network latency, real
PALI error codes, delta interaction with PALI encryption) are untested in model tests.
There is no Evergreen disagg variant for model tests. This is a test infrastructure
gap that blocks a whole category of higher-fidelity tests.
**Suggested test:** Implement the configurable DoD first. Then add a model test
Evergreen task that runs with `page_log=palite` as a minimum viable alternative, to
smoke-test model tests against a local implementation different from PALM.
**Already in existing analysis?** No — NEW GAP

---

### [WT-16134] — Enable test/format to run using PALI instead of PALite
**Type/Priority/Status:** Task / Major-P3 / Open
**Jira context:** PALite has severe throughput limitations: ~5–10% of classic speed,
cannot run parallel jobs, limited table count in test/format, makes long-running
tests impractical. The goal is to replace most test/format disagg tasks with PALI
to get realistic performance and parallelism. Currently all evergreen_disagg.yml
format tasks use PALite.
**Gap:** Long-running and stress tests (which are most likely to expose races,
corruption under delta chains, or block manager misaccounting) do not run under
PALI. Bugs that only manifest at higher throughput or with concurrent writers
(which PALite serializes) are invisible in CI.
**Suggested test:** (Infrastructure change, not a new unit test.) Add one or more
Evergreen tasks that run `test/format` with `page_log=pali` and parallelism enabled.
At minimum add a sanity task to detect obvious regressions.
**Already in existing analysis?** No — NEW GAP (test infrastructure)

---

### [WT-15266] — Dump all pages from the PALI response in the results array on checksum failure
**Type/Priority/Status:** Task / Major-P3 / Open
**Jira context:** When a checksum error occurs in disagg mode, only the failing page
is currently dumped. Because a read returns a full-page + N deltas in a results
array, the other entries in the array (preceding deltas) are not dumped and are lost.
A related PR (WT-16261) added infrastructure for multi-dump support in the log
decoder. This task is to wire the dump into the block_disagg_read path.
**Gap:** No test injects a checksum failure into a delta-chain read (where PALI
returns base + multiple deltas) and verifies that all entries in the results array
are dumped to the error log. The `test_verify_disagg.py` and `test_verify_disagg02.py`
tests do not simulate multi-part read corruption.
**Suggested test:** Using PALite (which is local), corrupt a specific delta in the
chain (e.g., flip a byte in the stored image), trigger a read, and assert that the
error message/dump contains hexdump output for ALL entries in the results array, not
just the corrupt one.
**Already in existing analysis?** No — NEW GAP

---

### [WT-15419] — Log error messages when PALI API call fails
**Type/Priority/Status:** Improvement / Major-P3 / Open (reserved for new hire)
**Jira context:** Currently, when any PALI API call fails, the error code is returned
but no human-readable error message is logged before propagation. This makes debugging
silent failures in CI very difficult.
**Gap:** This is related to the already-identified CS-H7 gap (page-log write error
fault injection). Even beyond fault injection, there is no test that verifies a failed
PALI call produces a diagnostic log line. Tests that inject errors (via PALite
instrumentation or mocking) do not check the log output. The gap here is specifically
in log message coverage: error propagation is tested, but log content is not.
**Suggested test:** Inject a PALite put failure (e.g., via an env var or PALite
hook), capture the connection's verbose error log, and assert the log contains a
message identifying the failed PALI function and the table/page affected. This would
also serve as a regression test for WT-15419 once implemented.
**Already in existing analysis?** Partially related to CS-H7 (error propagation
untested) — the log message dimension is a NEW sub-gap not covered by CS-H7.

---

## Uncertain Cases (may or may not be tested)

### [WT-14879] — Support generating page deltas for fast truncates
**Reason for uncertainty:** Fast truncate tests (`test_layered_fast_truncate01/02/03.py`)
exist and are non-trivial (commit/rollback/write-conflict coverage). However, none of
these files contain any reference to `delta` in their source. WT-14879 is in Backlog
and a comment from Alexander Gorrod asked whether this is even needed. The question
is whether the current behavior (delta disabled for truncated pages) is contractually
tested or just implicitly happens. `test_layered56.py` tests that page splits suppress
deltas, but there is no equivalent test asserting that fast truncates do not produce
unexpected deltas. The behavior is uncertain without running the code.

### [WT-16159] — Enable multi-process DB access in PALite
**Reason for uncertainty:** PALite's lack of multi-process support means the
leader-follower switch scenario (two processes simultaneously holding a connection)
cannot be tested via PALite. Some leader/follower switch tests exist
(`test_layered07.py`, `disagg_switch_follower_and_leader`), but these appear to use
sequential open/close rather than truly concurrent processes. Whether this gap is
covered by existing model tests or PALI-based CI is unclear without a full Evergreen
task audit.

### [WT-16224] — Unpack internal page deltas progressively during the merging process
**Reason for uncertainty:** This is an algorithmic optimization to the delta merge
path — changing from "unpack all, then merge" to "progressive unpack." The correctness
of the final page image should be identical either way. `test_layered32.py`
(`test_internal_page_delta_split_internal`) and `test_layered63.py` cover internal
page delta correctness, but it is not clear whether these tests run against the
optimized code path or the existing path. A future regression in the optimized
path may not be detectable if the tests pass with either implementation.

### [WT-15027] — Add heuristic to consider building a delta if a percentage of page rows are modified
**Reason for uncertainty:** This improvement would add a new delta eligibility
heuristic. As of the last comment (Dec 2025), "This is not urgent and it is unclear
whether we should do this. Move to the backlog." If implemented, it would need tests
asserting the threshold boundary (e.g., 49% modified → delta, 51% modified → full
page). Not currently testable because the heuristic does not exist yet.

---

## No Testing Gap (informational notes only)

- **WT-14591**: Remove deprecated PALI interfaces — pure refactoring; no test-observable behavior change.
- **WT-14592**: Ensure PALI compile fails gracefully on Windows — build-system issue; testing is by inspecting compile output.
- **WT-14772**: Add comments to PALI function args — documentation only; no runtime behavior.
- **WT-14873**: Add fine-grained latency metrics for PALI reads — new metric; once implemented it will need a stat-assertion test, but not a current gap.
- **WT-14950**: Update PALI doc post-discard verify implementation — documentation only.
- **WT-15026**: Investigate disk image reuse optimization — research/exploration task; no testable surface until implemented.
- **WT-15092**: Deprecate checkpoint IDs from PALI — API change; impacts tests when landed but not a current gap.
- **WT-15190**: Investigate `uint8_t` for delta count in PALI API — internal type change; not yet implemented.
- **WT-15194**: Use same macro for unpacking full pages and deltas — code deduplication; no behavior change.
- **WT-15709**: Support generating page deltas for page splits — unimplemented feature (Backlog); `test_layered56.py` already contractually tests the restriction (splits produce no delta). Once implemented, test needed.
- **WT-16525**: Remove `WT_PAGE_LOG_LSN_MAX` — cleanup; the constant is only defined in `wiredtiger.h.in` and not used in tests; removal is safe.
- **WT-16668**: Determine cause of PALite indirect leak in LSan — investigation task; no new test surface.
- **WT-16806**: Enable Windows build for PALite — build platform support; no functional test gap on Linux.
- **WT-17341**: Add `wt util` subcommand to read a single page through `WT_PAGE_LOG` — new tool sub-command; no existing test to gap-against since the feature does not exist yet.

---

## New Testing Areas (major themes not in existing analysis)

### Theme 1: Stats correctness for delta paths
Multiple tickets (WT-14555, WT-14504) point to a systemic problem: statistics are
asserted for delta *counts* but never for *bytes*. No disagg test ever checks
`block_byte_read`, `block_byte_write`, or `disagg_block_hs_byte_read`. This is
distinct from the already-identified CS-H2 gap (counters not asserted for
`disagg_block_get`/`disagg_block_put`). The byte-level stats require special care
because delta reads are multi-part (base + N deltas), and the summing logic is easy
to get wrong silently. A dedicated stats-verification test for the delta read/write
path would catch an entire class of silent regressions.

### Theme 2: Delta eligibility boundary conditions
Three tickets (WT-16442 re-split, WT-16239 high-delete, WT-14879 fast-truncate) each
describe a scenario where delta generation is suppressed or inverted. Currently only
the page-split suppression (WT-14879's existing behavior) is contractually tested.
The high-delete inversion (WT-16239) and re-split boundary (WT-16442) are completely
untested. A gap-filling approach is to write one test per boundary condition that
asserts the suppression stat (`rec_page_delta_rejected_*`) or verifies a full-page
was written when a delta was expected to be rejected.

### Theme 3: wt utility tool with disagg configurations
WT-15940 and WT-17341 together reveal that the `wt` command-line tool has no test
coverage for disagg-specific scenarios. WT-15940 is an open bug with a defined test
requirement (`self.runWt()`). WT-17341 will add a new subcommand. Neither scenario
has a test. This is a whole tool-level surface that is unexercised in the test suite.

### Theme 4: Encryption flag correctness through PALI
WT-16535 surfaces a critical correctness concern: the `WT_PAGE_LOG_ENCRYPTED` flag
must be set per table-type to distinguish user tables (encrypted) from internal
bootstrap tables (not encrypted). This flag is set in `block_disagg_write.c` but no
test verifies it is set correctly for all table types. A bug here causes startup
failures that would be very hard to diagnose in production. This is an
encryption-integration gap entirely absent from the existing analysis.

### Theme 5: Test infrastructure gaps blocking higher-fidelity testing
WT-15684 (model tests locked to PALM) and WT-16134 (test/format locked to PALite)
are not unit-test gaps but CI-infrastructure gaps that prevent entire categories of
tests from running under realistic conditions. Until these are resolved, bugs that
only manifest under real PALI semantics (true concurrency, real latencies, real error
codes) cannot be caught in the test suite.
