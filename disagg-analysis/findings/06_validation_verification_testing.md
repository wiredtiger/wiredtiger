# Gap Analysis: Validation / Verification / Version Cursors / Testing Infrastructure
Generated: 2026-05-06

Ticket source: 116 tickets from `/tmp/agent_g6.txt`, triaged below.
Jira queries: 20 tickets queried in detail.
Code search: FIXMEs in `src/cursor/`, `src/btree/bt_vrfy.c`, `src/conn/conn_layered.c`, `src/block/block_ckpt.c`, `src/block_disagg/`.

---

## Confirmed Testing Gaps

### [WT-15476 / WT-17189 / WT-17190 / WT-17192] — GC verify: ingest vs stable vs HS cross-check
**Type/Priority/Status:** Epic (WT-15476, Open, P3) + three child Tasks (all Open)
**Jira context:**
- WT-15476 is the parent epic (assigned Jasmine Bi, due 2026-05-15, `ds_durability_high_risk` label). It calls for verifying that every ingest record garbage-collected at GC time actually exists in the stable checkpoint.
- WT-17189 (Open, PR in progress): verify the most recent update against the stable table in **debug builds** during GC. Implementation is actively undergoing testing; several failure modes have been uncovered (lazy page-fetch race, delete-sentinel handling, `preserve_prepared` guard). No test yet that deliberately triggers a mismatch and expects the panic.
- WT-17190 (Open, no PR): after WT-17189, verify older updates against the **HS**. Scope not yet defined (how many entries to check).
- WT-17192 (Open, no PR): extend the debug-build check to **release builds** via 1-in-N random sampling. Frequency TBD.

**Gap:** No test deliberately introduces an ingest-vs-stable data mismatch and asserts that the GC verify fires. The implementation (WT-17189) is still changing; once stable a test that injects a mismatch (e.g. corrupt the stable checkpoint, then trigger GC) would confirm the diagnostic catches it. There is no test for the HS cross-check (WT-17190) at all.

**Suggested test:**
1. Leader: write key K at ts T, checkpoint to stable.
2. On the follower: manually remove K from stable (or use PALite/PALM fault injection) while K still sits in ingest.
3. Trigger GC → assert panic or WT_ERROR is observed (debug build).
4. Separately, write a key with multiple historic values, let them be evicted to HS, then corrupt the HS entry and verify the WT-17190 check fires.

**Already in existing analysis?** Yes — referenced as SO-H4 / SO-H5 in `05_scenario_analysis/`. However the details of the three child tasks (debug-build, HS cross-check, release-build sampling) are **NEW** specifics not in prior analysis.

---

### [WT-15064] — Table corruption detection tests for DisAgg shared tables
**Type/Priority/Status:** Task / P3 / Open — unassigned; Epic link WT-16720; 5 commits in dev history
**Jira context:** Current corruption tests work by writing invalid data to local `.wt` files, which does not work for shared tables stored in PALI/PALM. Two proposed approaches: (1) use PALM Python wrapper to overwrite a targeted page; (2) expose a dedicated C testing API. No implementation merged yet.

**Gap:** No test exercises the scenario where `session.verify()` detects corruption in a disagg shared table page stored remotely. This is the fundamental "verify actually catches corruption" test for disagg.

**Suggested test:** Use the PALM Python wrapper (approach 1) to zero-fill a data page for a known key in the shared table; call `session.verify()` and assert it returns an error / specific verification failure message. Run in both leader and follower roles.

**Already in existing analysis?** No — NEW GAP.

---

### [WT-17250] — Validation test for shared disk cache
**Type/Priority/Status:** Task / P3 / Open — unassigned (Backlog); no PR
**Jira context:** Run WT with shared disk cache enabled for 10–20 minutes, then walk every hash table entry and check for dangling entries (refcount 0, or page no longer in cache). Test does not yet exist.

**Gap:** Shared disk cache correctness is not tested at all via a self-validating stress test.

**Suggested test:** A C-level or Python long-running test that: (a) enables shared disk cache, (b) runs concurrent reads/writes/evictions for N minutes, (c) at the end calls an internal diagnostic walk of the hash table and asserts no dangling entries. Could be gated behind a debug-mode compile flag.

**Already in existing analysis?** No — NEW GAP.

---

### [WT-17146] — Add shared metadata consistency check to verify
**Type/Priority/Status:** Task / P3 / Backlog — unassigned; Epic link WT-16720
**Jira context:** In disagg mode, metadata lives in both local `WiredTiger.wt` and shared `WiredTigerShared.wt_stable`. The verify path does not cross-check that (a) every shared entry has a local match and (b) every local file entry has a shared counterpart. No implementation or PR exists.

**Gap:** `session.verify()` in disagg mode makes no assertion about shared metadata completeness. A table could exist in shared metadata but be missing locally (or vice versa) without verify catching it.

**Suggested test:** Write a Python test that manually inserts a stale/orphan entry into `WiredTigerShared.wt_stable` via the metadata cursor, then calls `session.verify()` and asserts WT_ERROR or a specific corruption message.

**Already in existing analysis?** No — NEW GAP.

---

### [WT-17188] — Extend btree ID uniqueness verification to shared metadata
**Type/Priority/Status:** Task / P3 / Open — unassigned; Epic link WT-16720
**Jira context:** WT-17116 added a local metadata btree-ID uniqueness scan. WT-17188 extends this to shared metadata (`WiredTigerShared.wt_stable`). Two known blockers: (1) opening a checkpoint cursor on shared metadata during verify causes a `WT_WITH_CHECKPOINT_LOCK` re-entrancy deadlock, (2) direct PALite read of shared metadata hits infinite-retry `SQLITE_NONE`. PoC PR #13525 exists but is blocked.

**Gap:** Duplicate btree IDs in shared metadata are not caught by verify. This can propagate corruption to every node picking up the checkpoint.

**Suggested test:** Inject a duplicate btree ID into `WiredTigerShared.wt_stable` metadata, then call `session.verify()` on the affected table and assert it returns WT_ERROR. (Blocked by the deadlock/PALite issues; the test design should track these blockers.)

**Already in existing analysis?** No — NEW GAP. (WT-17127 is a related minor variant: `skip_hs` check in `bt_vrfy.c` uses `strcmp(name, WT_METAFILE_URI)` instead of `WT_IS_URI_METADATA`, which may miss the shared metadata URI. This is also untested.)

---

### [WT-17125] — verify() should continue past read errors in disagg
**Type/Priority/Status:** Task / P3 / Backlog — unassigned; Epic link WT-16720
**Jira context:** `bt_vrfy.c` has `read_corrupt` mode that continues past page-read failures (lines 977–1048). This mode is not verified to work end-to-end in disagg when PALI/SLS cannot return a remote page. No test exercises this path. The ticket also notes this must work through the MongoDB `validate` command.

**Gap:** If any remote page is temporarily unavailable, verify aborts the whole scan instead of continuing with `read_corrupt=true`. No test confirms `read_corrupt` works in disagg context.

**Suggested test:** Use PALI fault-injection (or mock) to make one specific page return a read error, then call `session.verify(uri, "read_corrupt=true")` and assert: (a) no panic, (b) returned error is WT_ERROR (not WT_PANIC), (c) remaining pages are still verified.

**Already in existing analysis?** No — NEW GAP. Related to existing SO-H4 (verify on follower) but distinct.

---

### [WT-17278] — Follower remove returns WT_NOTFOUND where leader returns WT_ROLLBACK
**Type/Priority/Status:** Bug / P3 / Open — backlog; assigned to SE Foundations sprint 2026-06-05
**Jira context:** In multi-node predictable replay (`test/format disagg.mode=multi`), the leader calls `__wt_btcur_remove` which triggers `__curfile_update_check` and returns `WT_ROLLBACK` when an invisible committed update sits above a visible tombstone. The follower path (`__clayered_remove_follower`) calls `__clayered_lookup`, sees only the visible tombstone, and returns `WT_NOTFOUND`. This divergence causes hash mismatches in multi-node validation. A Python reproducer exists in the Jira comments.

**Gap:** The follower remove path does not detect invisible committed updates in the update chain above a visible tombstone, causing false-negative write-conflict detection. This is a correctness bug in `cur_layered.c` with no test specifically covering the case. The Python reproducer posted in comments is not yet a committed test.

**Suggested test:** The Python reproducer from the ticket comments should be added as `test_layered_remove_notfound.py`. The test should: insert key at ts=100, tombstone at ts=200, re-insert at ts=300, checkpoint; then on follower open at read_ts=250 and call `cursor.remove()` — assert `WT_ROLLBACK` (once the bug is fixed; currently asserts `WT_NOTFOUND` as the known-broken state).

**Already in existing analysis?** No — NEW GAP.

---

### [WT-17247] — Layered cursor writes on follower do not check stable cell's full time window
**Type/Priority/Status:** Bug / Critical P2 / Open — tagged `expedite`; SE Foundations sprint 2026-05-22
**Jira context:** `__clayered_remove_follower`, `__clayered_insert`, and `__clayered_update` on the follower all use session-visibility (`read_ts`) to decide if a key exists on stable. A committed `stop_ts` on the stable cell that is invisible at `read_ts` but honored by the drain causes a mismatch: the follower writes to ingest for a key the drain considers absent, triggering `__layered_assert_tombstone_has_value_on_stable_btree`. Reproducer uses `disagg.mode=switch` with `ops.prepare=1`. The drain-time assertion (WT-17240) surfaces this.

**Gap:** There is no test that specifically exercises the follower write path when a stable cell has an invisible `stop_ts`. The three affected write operations (remove, insert-existence check, update/modify) each lack a targeted test for this scenario.

**Suggested test:** Three scenario tests (one per write type): write key K with commit_ts=T1, set stable checkpoint, then on follower open with read_ts=T0 < T1 and attempt (a) remove, (b) insert with no-overwrite, (c) update/modify — assert correct return codes and that no drain assertion fires. These should also be run with `preserve_prepared=true`.

**Already in existing analysis?** No — NEW GAP (though WT-17240 / drain assertion is related; the test gap for the root cause is here).

---

### [WT-16136] — Version cursor: stop durable timestamp ambiguity for HS entries
**Type/Priority/Status:** Technical Debt / P3 / Backlog — unassigned; FIXME in `src/cursor/cur_version.c:665`
**Jira context:** When iterating HS entries with a version cursor, the `stop_durable_ts` field can come from either a tombstone or a previous full value. The code cannot distinguish these cases. There is a `FIXME-WT-16136` comment at `cur_version.c:665`.

**Gap:** Version cursor behavior for HS entries with ambiguous stop timestamps is unspecified and untested. No test exercises a version cursor over a key that has both a tombstone and multiple historic versions in the HS.

**Suggested test:** Build on the existing `test_hs34`-style test from the WT-16148 description: insert key A, update multiple times, evict to HS, then open a version cursor and assert the returned stop_durable_ts values are correct for each HS entry (tombstone vs full value).

**Already in existing analysis?** No — NEW GAP.

---

### [WT-16148] — Version cursor cannot access orphaned HS entries
**Type/Priority/Status:** Task / P3 / Open — unassigned; `cur_version.c` component
**Jira context:** When a key is deleted with `use_timestamp=false` and the DB is reopened and the same key is re-inserted, the old HS entry becomes "orphaned." A version cursor on the file cannot see the orphaned entry through `next()`, returning `WT_NOTFOUND` prematurely. A concrete reproducer (`test_hs34.test_hs34.test_hs_recovery`) is in the ticket. The issue is flagged as relevant to MongoDB debugging since version cursors are used as debug tools.

**Gap:** Orphaned HS entries are invisible to version cursors. No test verifies the version cursor's behavior on keys with orphaned HS entries. The reproducer in the ticket is not in the test suite.

**Suggested test:** Commit the reproducer from WT-16148's description as `test_hs34.py` (or similar). Add assertions for both the "should see entry" case and the "correctly reports no entry" case, with a clear comment marking the current broken behavior.

**Already in existing analysis?** No — NEW GAP.

---

### [WT-16118] — Periodic readback and validation of WT pages on the primary
**Type/Priority/Status:** Task / P3 / Open — unassigned; `ds_durability_medium_risk` label
**Jira context:** Pages may be corrupted in transit (primary→LogServer) or at rest (PageServer). There is no periodic readback that would catch such corruption on the primary node itself. Proposal: occasionally sample random pages and validate checksums. Discussion ongoing whether to implement above or below the WT layer.

**Gap:** No automated test verifies that checksum validation fires when a page is corrupted in the PALI/PALM layer after write but before the readback check.

**Suggested test:** Fault-inject a page write in PALM to corrupt the checksum, then trigger a readback validation (once the feature exists) and assert the error is surfaced. This is a future test dependent on the feature being built.

**Already in existing analysis?** No — NEW GAP (future/feature-dependent).

---

### [WT-14915] — DisAgg verification from other component perspectives
**Type/Priority/Status:** Task / P3 / Open — unassigned (Backlog); Story points: 13
**Jira context:** The block manager has less control in disagg (no local extent lists). HS verification (FIXME-WT-10779 in `bt_vrfy.c:1267`) is explicitly disabled. Log server and PALI have not been analyzed for what cross-verification they could provide. The ticket is intentionally left open-ended.

**Gap:** History store verification is disabled for disagg (`FIXME-WT-10779`). There is no test that exercises HS verification path in disagg mode at all. Block manager extent-list verification equivalents for disagg are undefined.

**Suggested test:** No single test can cover this until the verification paths are defined. Immediate actionable gap: enable HS verification (remove FIXME guard at `bt_vrfy.c:1267`) in disagg debug builds and add a test that calls `session.verify()` on a layered table that has data in the HS, asserting no crash and correct output.

**Already in existing analysis?** Yes — partially in `07_verification.md` (SO-H5). The specific `FIXME-WT-10779` code point is **NEW**.

---

### [WT-16113] — Leader data validation not integrated into main format stress test
**Type/Priority/Status:** Improvement / P4 / Open — unassigned
**Jira context:** The existing `format-stress-data-validation-test-disagg-leader` task runs as a standalone Evergreen variant, not integrated into `format-stress-test-disagg-leader`. This means when format's main stress test runs, it does not exercise the mirrored-table comparison. Proposal: integrate mirror table validation into the main stress run and remove the standalone task.

**Gap:** The mirror table (leader vs non-layered) validation runs in a separate Evergreen task that is not always triggered. Bugs that only manifest under concurrent stress + data comparison may be missed.

**Suggested test:** This is an infrastructure gap — modify `test/format` CONFIG.disagg and Evergreen YAML to enable mirror-table comparison at 50% probability within the main disagg stress run.

**Already in existing analysis?** No — NEW GAP (test infrastructure).

---

### [WT-15404] — Python tests skip disagg due to logged table config
**Type/Priority/Status:** Task / P3 / Open — unassigned; Story points: 5
**Jira context:** Many Python tests set `log=(enabled)` or `log=(enabled=true)` even when logging is not the test's purpose. Because logged tables are unsupported in disagg, these tests are auto-skipped under `hook_disagg.py`, reducing disagg coverage significantly. The ticket calls for an audit to remove unnecessary `log=` configs.

**Gap:** Unknown number of Python tests that test non-logging behavior are silently skipped under disagg due to incidental `log=` config. Actual coverage loss is unquantified.

**Suggested test:** Enumerate all tests with `log=` configs that are currently skipped under disagg. For each, determine if the log config is essential. Those where it is not should be split into a variant without `log=` that runs under disagg. This is a test infrastructure gap.

**Already in existing analysis?** No — NEW GAP (test infrastructure/coverage).

---

## Uncertain Cases

### [WT-16260] — Expired history testing
**Status:** Backlog / P3 — description is minimal ("make sure the previous three WT tickets were adequately tested"). The referenced tickets are not named in the ticket itself. Dependency is `SERVER-115340` (snapshot read test coverage for disagg, Open/unassigned).
**Verdict:** UNCERTAIN — cannot assess gap without knowing which three tickets are referenced. Likely overlaps with HS validation but scope is too vague.

### [WT-17160] — Cache stuck in test_layered91 with 6-key tables
**Status:** Backlog / P3 / Bug — increasing max table states from 5→6 keys (11011 tables) causes "Cache stuck for too long" abort in `WiredTigerSharedHS.wt_stable` eviction server.
**Verdict:** UNCERTAIN — this is a scalability/eviction bug that limits test breadth rather than a standalone test gap. If fixed, it would allow a broader combinatorial verification sweep. Not a pure testing gap but a blocker for higher-coverage testing.

### [WT-17127] — bt_vrfy.c uses `strcmp(name, WT_METAFILE_URI)` instead of `WT_IS_URI_METADATA`
**Status:** Backlog / P3 — the `skip_hs` logic in verify uses a string compare that may not cover all metadata URI forms in disagg. Ivan Kochin's comment notes the HS assertion still exists in `cur_hs.c:1378` and the failure mode is not fully understood.
**Verdict:** UNCERTAIN — the bug may be real but consequences in disagg are unclear. The fix is small; no targeted test currently covers this code path in disagg.

### [WT-16734] — Enable disagg testing (-G) for schema abort tests
**Status:** Open / P3 — enable crash recovery testing for disagg. Not in the queried list but appears relevant.
**Verdict:** UNCERTAIN — described as infrastructure work. Gap exists but is already tracked.

---

## No Gap / Build-Failure / Infrastructure Only (brief list)

The following tickets are either pure CI build failures, performance regressions, or infra work with no actionable test gap beyond what the ticket itself describes:

- **WT-14361, WT-14713, WT-15578, WT-16127, WT-16129, WT-16149, WT-16238, WT-16276, WT-16277, WT-16439, WT-16474, WT-16541, WT-16549, WT-16553, WT-16586, WT-16692, WT-16855, WT-16856, WT-16864, WT-16899, WT-17008, WT-17023, WT-17205, WT-17316, WT-17340, WT-17367** — Build failures / CI flakes. Each describes a specific test failing, not a systematic gap.
- **WT-14232** — many-collection test using private mongo repo (infra).
- **WT-14416, WT-14420, WT-14434, WT-14435, WT-14436, WT-14440** — High-level stories (functional parity, performance, security). No specific test gap.
- **WT-14964** — Segfault in snappy compress / tcmalloc (build failure).
- **WT-15057** — Turtle file atomic update (not test gap).
- **WT-15189** — Python timeout in `clayered_next_random` (build failure).
- **WT-15227** — Enable precise checkpoints in disagg hook (bug/infra).
- **WT-15261** — Add switching mode to `test/checkpoint` (infrastructure task).
- **WT-15313** — Add disagg to wtperf (nice-to-have).
- **WT-15364** — Add model tests to PR testing (infra).
- **WT-15434** — Performance regression (perf change point).
- **WT-15446** — Do not print large oplog in test_layered23 (cosmetic).
- **WT-15672** — ASAN tests with debug mode (infra improvement).
- **WT-15770** — Fix TSAN warnings (infra).
- **WT-15788** — test/format multi-node checkpoint metadata (infra task).
- **WT-15790** — Tag long-running layered tests (cosmetic).
- **WT-15950** — Enable MSan builds (infra).
- **WT-16072** — Failed assert layered63 test (build failure).
- **WT-16155** — Add reopen support to `format_test_script` (infra).
- **WT-16197** — Add Python disagg tests to code coverage tracking (infra).
- **WT-16226** — Use `--skip-tests-in-file` flag in Python runner (infra).
- **WT-16256** — Python unit test framework for `wt_binary_decode` (infra).
- **WT-16478** — Create verify section in architecture guide (docs).
- **WT-16481** — test/format multi-node with database reopen (bug/infra).
- **WT-16535** — Ensure WT_PAGE_LOG_ENCRYPTED is set for regular tables (task).
- **WT-16736** — Enable test/format disagg multi-node in Evergreen variants (infra).
- **WT-16775** — Investigate disagg config in model workload generator (minor task).
- **WT-16824** — Refactor verify string helpers to return error codes (tech debt).
- **WT-16873** — Fix unintentional skipping of non-tiered tests (bug/infra).
- **WT-16885** — Prefetch during disagg perf testing (task).
- **WT-16918** — Implement `tableExists()` for disagg Python tests (infra).
- **WT-16931** — Add metadata helpers to Python test framework (infra).
- **WT-17099** — test_layered71 build failure (BB-Tools).
- **WT-17147** — Add diagnostics to test/format data mismatch failures (investigation task).
- **WT-17224, WT-17225, WT-17226** — The parent testing-gaps analysis tasks (meta).
- **WT-17327** — Document stable schema epoch (docs).
- **WT-17338** — Auto-pick latest checkpoint in disagg follower for wt tool (sub-task).
- **WT-17344** — Add wt util subcommand to dump turtle page (sub-task).
- **WT-17380** — Enable prepare for test/format disagg switch mode (infra task).
- **WT-14507** — Extend cursor bound testing for layered tables (separate from verification).
- **WT-14543** — Enhance layered cursor testing with oplog emulation (separate area).
- **WT-14548** — Validate basic failover works for MDB/WT (integration test).
- **WT-14788, WT-14795, WT-14830** — Misc test improvements (block cache, prepared atomicity).
- **WT-14938, WT-14939** — Layered tests not working for tiered storage (compatibility).
- **WT-14950** — Update PALI doc (docs).
- **WT-14993** — Tidy up known-bad disagg tests (cleanup).
- **WT-14998** — Re-enable layered tables on truncate tests (re-enable).
- **WT-15025** — Measure code coverage for recovery in test/model (coverage tooling).
- **WT-15040** — Enable prepared transactions in test/model (model work).
- **WT-15055** — Build failure on ubuntu2004 (BB-Tools).
- **WT-15294** — test_prepare20.py crash in checkpoint (bug, separate area).
- **WT-15369, WT-15371, WT-15372** — Fix specific disagg test failures (cursor stats, hs01, verbose01).
- **WT-15417** — Fix dropUntilSuccess errors (bug).
- **WT-15475** — Truncate invalid argument in test/format disagg leader (bug).
- **WT-15530** — Fix WT_MODIFY memory buffer error in test/format (bug).
- **WT-15612** — Merge straggling tests to develop (cleanup).
- **WT-15684** — Make PALI configurable in test/model (model work).
- **WT-16134** — Enable test/format to run using PALI instead of PALite (infra).
- **WT-16529** — (not in list, mentioned in context of WT-17127).

---

## Summary of New Gaps by Priority

| Ticket(s) | Gap Description | Priority | Existing Coverage |
|-----------|----------------|----------|-------------------|
| WT-17189/17190/17192 | GC verify: ingest-vs-stable-vs-HS mismatch detection | HIGH | Partial (no test for mismatch injection) |
| WT-17247 | Follower write path ignores stable cell's invisible stop_ts | HIGH (Critical P2) | None |
| WT-17278 | Follower remove: WT_NOTFOUND vs WT_ROLLBACK asymmetry | HIGH | None (reproducer in ticket comments only) |
| WT-15064 | Corruption detection test for disagg shared table pages | HIGH | None |
| WT-17146 | Shared metadata consistency check in verify | MEDIUM | None |
| WT-17188 | Btree ID uniqueness check in shared metadata | MEDIUM | Partial (local-only check exists in WT-17116) |
| WT-17125 | verify read_corrupt mode end-to-end in disagg | MEDIUM | None |
| WT-17250 | Shared disk cache validation test | MEDIUM | None |
| WT-16148 | Version cursor cannot see orphaned HS entries | MEDIUM | None (reproducer unpublished) |
| WT-16136 | Version cursor stop_durable_ts ambiguity for HS | LOW | None (FIXME in cur_version.c:665) |
| WT-16118 | Periodic page readback validation on primary | LOW (feature-dep) | None |
| WT-14915 | HS verification disabled in disagg (FIXME-WT-10779) | MEDIUM | Partial (SO-H5 noted the gap) |
| WT-16113 | Leader data validation not in main format stress run | MEDIUM | Separate Evergreen task only |
| WT-15404 | Many Python tests silently skipped due to log= config | MEDIUM | None (unquantified) |

---

## FIXMEs in Verification / Cursor Code (code evidence)

The following in-code FIXMEs directly correspond to test gaps:

| Location | FIXME | Linked Ticket | Impact |
|----------|-------|---------------|--------|
| `src/btree/bt_vrfy.c:1267` | `FIXME-WT-10779 - Enable the history store validation` | WT-14915 | HS verification is completely skipped in disagg |
| `src/cursor/cur_version.c:665` | `FIXME-WT-16136: for history store, it is hard to determine if the stop durable timestamp is from a tombstone or the previous full value` | WT-16136 | Version cursor ambiguity; no targeted test |
| `src/conn/conn_layered.c:391` | `FIXME-WT-14730: verify that there is no btree ID conflict` | WT-17188 (related) | btree ID conflict check not yet implemented in conn_layered; verify is also incomplete |
| `src/conn/conn_layered.c:363` | `FIXME-WT-14730: check that the other parts of the metadata are identical` | WT-17146 | Shared metadata cross-check not done |
| `src/block/block_ckpt.c:120` | `FIXME: We may need to change how we setup for verify when it supports tiered tables` | WT-14915 | Verify setup for disagg is incomplete |
| `src/block_disagg/block_disagg_read.c:153` | `FIXME-WT-15768: To support current testing, we never give up. It is better to hang here` | — | Read error handling prevents verify continue-past-errors |
